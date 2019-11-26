var cc = DataStudioApp.createCommunityConnector();

function getConfig() {
  var config = cc.getConfig();

  config.newInfo()
      .setId('instructions')
  .setText('Please enter the configuration data for your Facebook connector');

  config.newTextInput()
      .setId('page_id')
      .setName('Enter your Facebook Page Id')
      .setHelpText('Find the page Id on the \'About\' section of your page')  
      .setPlaceholder('Enter Facebook Page Id here')
      .setAllowOverride(false);
  
  config.setDateRangeRequired(true);

  return config.build();
}

function getFields() {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;  
  
  fields.newDimension()
      .setId('postDate')
      .setName('Date')
      .setType(types.YEAR_MONTH_DAY);
  
  fields.newDimension()
      .setId('postId')
      .setName('Post ID')
      .setType(types.TEXT);  

  fields.newDimension()
      .setId('postMessage')
      .setName('Post Message')
      .setType(types.TEXT);  

  fields.newDimension()
      .setId('postLink')
      .setName('Link to post')
      .setType(types.URL);    
  
  fields.newMetric()
      .setId('postLikes')
      .setName('Likes on post')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('postComments')
      .setName('Comments on post')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('postShares')
      .setName('Shares on post')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pageLikes')
      .setName('Likes on page')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
  return fields;
}


function getSchema(request) {  
    var fields = getFields().build();
    return { 'schema': fields };    
}

// Get data from Facebook Graph API
function graphData(request, query) {
  var pageId = request.configParams['page_id'];
  var requestEndpoint = "https://graph.facebook.com/v5.0/"+pageId+"/"
  
  //Get page access token
  var tokenUrl = requestEndpoint+"?fields=access_token";
  var tokenResponse = UrlFetchApp.fetch(tokenUrl,
      {
        headers: { 'Authorization': 'Bearer ' + getOAuthService().getAccessToken() },
        muteHttpExceptions : true
      });
  var pageToken = JSON.parse(tokenResponse).access_token;
  
  // Perform API Request
  var requestUrl = requestEndpoint+query+"&access_token="+pageToken;
  
  var response = UrlFetchApp.fetch(requestUrl,
      {
        muteHttpExceptions : true
      });
  
  var parseData = JSON.parse(response);
  
  return parseData;

}


function getData(request) {    
  
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  
  var requestedFields = getFields().forIds(requestedFieldIds);
  
  var startDate = new Date(request['dateRange'].startDate);
  var endDate = new Date(request['dateRange'].endDate);
  
  var timeRangeSince = startDate.toISOString().slice(0, 10);
  var timeRangeUntil = endDate.toISOString().slice(0, 10);
  var timeRange = "&since="+timeRangeSince+"&until="+timeRangeUntil;
  
  // Determine if start date is the same as end date
  if (startDate.toISOString().slice(0, 10) == endDate.toISOString().slice(0, 10)) {
    timeRange = "&since="+timeRangeSince;
  }
  
  var postData = graphData(request, "posts?time_increment=1&fields=message,story,created_time,permalink_url,likes.summary(true),comments.summary(true),shares"+timeRange);
  var pageLikesData = graphData(request, "insights/page_fans?fields=values&since="+timeRangeUntil);
  
  var outputData = {};
  outputData.posts = postData;
  outputData.page_likes = pageLikesData;
  
  /*
  if(parseData.hasOwnProperty('error')){
    // TODO
  }  
  */
  
  //Logger.log(JSON.stringify(parseData));
  
  if(outputData.posts.data.length>0)
  {    
    rows = reportToRows(requestedFields, outputData);
    // TODO: parseData.paging.next != undefined
  } else {
    rows = [];
  }
  
  result = {
    schema: requestedFields.build(),
    rows: rows
  };  
  
  //cache.put(request_hash, JSON.stringify(result));
  return result;  
}


function reportPosts(report) {  
  var rows = [];
  
  // Loop posts
  for( var i = 0; i < report.data.length; i++) {
    
    // Define empty row object
    var row = {};
    row["id"] = report.data[i]['id'];
    
    //Return date object to ISO formatted string
    row["postDate"] = new Date(report.data[i]['created_time']).toISOString().slice(0, 10);
    
    row["postMessage"] = report.data[i]['message'] || report.data[i]['story'];
    row["postLink"] = report.data[i]['permalink_url'];
    row["postLikes"] = 0;
    
    // Determine if likes object exist
    if (typeof report.data[i]['likes'] !== 'undefined') {
      row["postLikes"] = report.data[i]['likes']['summary']['total_count'];
    }
    
    row["postComments"] = 0;
    
    row["postShares"] = 0;
    // Determine if shares object exist
    if (typeof report.data[i]['shares'] !== 'undefined') {
      row["postShares"] = report.data[i]['shares']['count'];
    }
    
    // Assign all post data to rows list
    rows.push(row);
  }
  return rows;
}

function reportPageLikes(report) {
  var rows = [];
  
  // Only report last number of page likes within date range
  var row = {};
  row["pageLikes"] = report.data[0].values[0]['value'];
  rows[0] = row;
  
  return rows;
  
}


function reportToRows(requestedFields, report) {
  rows = [];
  var postsData = reportPosts(report.posts);
  var pageLikesData = reportPageLikes(report.page_likes);
    
  // Merge data
  var data = postsData.concat(pageLikesData);
  console.log(JSON.stringify(data));

   
  for(var i = 0; i < data.length; i++) {
    row = [];    
    requestedFields.asArray().forEach(function (field) {
        console.log(data[i]["postDate"]);
        
      // Assign post data values to rows
      if (field.getId().indexOf('post') > -1 && typeof data[i]["postDate"] !== 'undefined') {
        switch (field.getId()) {
          case 'postDate':
            return row.push(data[i]["postDate"].replace(/-/g,''));
          case 'postId':
            return row.push(data[i]["id"]);
          case 'postMessage':
            return row.push(data[i]["postMessage"]);
          case 'postLink':
            return row.push(data[i]["postLink"]);
          case 'postLikes':
            return row.push(data[i]["postLikes"]);
          case 'postComments':
            return row.push(data[i]["postComments"]);
          case 'postShares':
            return row.push(data[i]["postShares"]);
        }
      }
      
      // Assign post data values to rows
      if (field.getId().indexOf('page') > -1) {
        return row.push(data[i]["pageLikes"]);
      }
      
    });
    if (row.length > 0) {
      rows.push({ values: row });
    }
  }
  
  return rows;
}


function isAdminUser(){
 var email = Session.getEffectiveUser().getEmail();
  if( email == 'steven@itsnotthatkind.org' ){
    return true; 
  } else {
    return false;
  }
}

/**** BEGIN: OAuth Methods ****/

function getAuthType() {
  var response = { type: 'OAUTH2' };
  return response;
}

function resetAuth() {
  getOAuthService().reset();
}

function isAuthValid() {
  return getOAuthService().hasAccess();
}

function getOAuthService() {
  return OAuth2.createService('exampleService')
    .setAuthorizationBaseUrl('https://www.facebook.com/dialog/oauth')
    .setTokenUrl('https://graph.facebook.com/v5.0/oauth/access_token')      
    .setClientId(CLIENT_ID)
    .setClientSecret(CLIENT_SECRET)
    .setPropertyStore(PropertiesService.getUserProperties())
    .setCallbackFunction('authCallback')
    .setScope('pages_show_list, manage_pages, read_insights');
};

function authCallback(request) {
  var authorized = getOAuthService().handleCallback(request);
  if (authorized) {
    return HtmlService.createHtmlOutput('Success! You can close this tab.');
  } else {
    return HtmlService.createHtmlOutput('Denied. You can close this tab');
  };
};

function get3PAuthorizationUrls() {
  return getOAuthService().getAuthorizationUrl();
}

/**** END: OAuth Methods ****/

