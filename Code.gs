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
  
  var parseData = graphData(request, "posts?time_increment=1&fields=message,story,created_time,permalink_url,likes.summary(true),comments.summary(true),shares"+timeRange);
  
  if(parseData.hasOwnProperty('error')){
    // TODO
  }  
  
  //Logger.log(JSON.stringify(parseData));
  
  if(parseData.data.length>0)
  {    
    rows = reportToRows(requestedFields, parseData);
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


function reportToRows(requestedFields, report) {  
  rows = [];
  
  for( var i = 0; i < report.data.length; i++){
    var row = [];
    var id = report.data[i]['id'];
    
    //Return date object to ISO formatted string
    var date = new Date(report.data[i]['created_time']).toISOString().slice(0, 10);
    var postMessage = report.data[i]['message'] || report.data[i]['story'];
    var postLink = report.data[i]['permalink_url'];
    var postLikes = 0;
    
    // Determine if likes object exist
    if (typeof report.data[i]['likes'] !== 'undefined') {
      postLikes = report.data[i]['likes']['summary']['total_count'];
    }
    
    var postComments = 0;
    // Determine if comments object exist
    if (typeof report.data[i]['comments'] !== 'undefined') {
      postComments = report.data[i]['comments']['summary']['total_count'];
    }
    var postShares = 0;
    // Determine if shares object exist
    if (typeof report.data[i]['shares'] !== 'undefined') {
      postShares = report.data[i]['shares']['count'];
    }
        
    requestedFields.asArray().forEach(function (field) {
      switch (field.getId()) {
          case 'postDate':
            return row.push(date.replace(/-/g,''));
          case 'postId':
            return row.push(id);
          case 'postMessage':
            return row.push(postMessage);
          case 'postLink':
            return row.push(postLink);
          case 'postLikes':
            return row.push(postLikes);
          case 'postComments':
            return row.push(postComments);
          case 'postShares':
            return row.push(postShares);
      }
    });
    
    rows.push({ values: row });
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

