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
  
  fields.newDimension()
      .setId('pageFansGender')
      .setName('Gender')
      .setType(types.TEXT);
  
   fields.newDimension()
      .setId('pageFansAge')
      .setName('Age')
      .setType(types.TEXT);
  
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
      .setName('Total Likes')
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
  
  // Set start and end date for query
  var startDate = new Date(request['dateRange'].startDate);
  var endDate = new Date(request['dateRange'].endDate);
  
  /*
  -------------------------------------------------------
  Create chunks of the date range because of query limit
  -------------------------------------------------------
  */
  
  var offset = 2; // Results are reported the day after the startDate and between 'until'. So 2 days are added.
  var chunkLimit = 93 - offset; // Limit of 93 days of data per query
  var daysBetween = (endDate.getTime() - startDate.getTime()) / (1000 * 3600 * 24); // Calculate time difference in milliseconds. Then divide it with milliseconds per day 
  
  console.log("query: %s, startDate: %s, endDate: %s, daysBetween: %s", query, startDate, endDate, daysBetween);
    
  // Split date range into chunks
  var queryChunks = [];
  
  // If days between startDate and endDate are more than the limit
  if (daysBetween > chunkLimit) {
    var chunksAmount = daysBetween/chunkLimit;
     console.log("ChunksAmount: %s", chunksAmount);
        
    // Make chunks per rounded down chunksAmount
    for (var i = 0; i < Math.floor(chunksAmount); i++) {
      // Define chunk object
      var chunk = {};
      
      // If no chunks have been added to the queryChunks list
      if (queryChunks.length < 1) {
        chunk['since'] = startDate;
        chunk['until'] = new Date(startDate.getTime()+(86400000*(chunkLimit+offset)));
              
      // If a chunk already is added to the queryChunks list
      } else {
        chunk['since'] = new Date(queryChunks[i-1]['until'].getTime()-(86400000*(offset-1))); // 'Until' has offset of 2 days. 'Since' should start 1 day after last date range chunk
        chunk['until'] = new Date(chunk['since'].getTime()+(86400000*(chunkLimit+offset-1)));
      }
      
      // Add chunk to queryChunks list
      queryChunks.push(chunk);
    }
    
    // Make chunk of leftover days if there are any
    if (chunksAmount - queryChunks.length > 0) {
      
      var leftoverDays = Math.floor((chunksAmount - queryChunks.length) * chunkLimit) // Decimal number * chunkLimit rounded down gives the amount of leftover days
      console.log("LeftoverDays: %s", leftoverDays);
      var chunk = {};
      chunk['since'] = new Date(queryChunks[queryChunks.length-1]['until'].getTime()-(86400000*(offset-1))); // 'Until' has offset of 2 days. 'Since' should start 1 day after last date range chunk
      chunk['until'] = new Date(chunk['since'].getTime()+(86400000*(leftoverDays + offset)));
      
      // Add chunk to queryChunks list
      queryChunks.push(chunk);
     
    }
    
  }
  // If days between startDate and endDate are less than or equal to the limit
  else {
      var chunk = {};
      chunk['since'] = startDate;
      chunk['until'] = new Date(endDate.getTime()+(86400000*offset)); //endDate + until offset in milliseconds
    
      // Add chunk to queryChunks list
      queryChunks.push(chunk);
  }
   
  /*
  ------------------------------------------------------
  Loop the chunks and perform the API request per chunk
  ------------------------------------------------------
  */
  
  
  /*
  //Get page access token
  var tokenUrl = requestEndpoint+"?fields=access_token";
  var tokenResponse = UrlFetchApp.fetch(tokenUrl,
      {
        headers: { 'Authorization': 'Bearer ' + getOAuthService().getAccessToken() },
        muteHttpExceptions : true
      });
  var pageToken = JSON.parse(tokenResponse).access_token;
  */
  
  //Use pageToken for testing purposes
  var pageToken = PAGE_TOKEN;
  
  // Define data object to push the graph data to
  var dataObj = {};
  
  
  // If posts object
  
  if (query.indexOf('posts') > -1) {
    // Set date range parameters
    var dateRangeSince = queryChunks[0]['since'].toISOString().slice(0, 10);
    var dateRangeUntil = queryChunks[queryChunks.length-1]['until'].toISOString().slice(0, 10);
    
    var dateRange = "&since="+dateRangeSince+"&until="+dateRangeUntil;
        
    // Perform API Request
    var requestUrl = requestEndpoint+query+dateRange+"&access_token="+pageToken;
    
    var response = UrlFetchApp.fetch(requestUrl,
      {
        muteHttpExceptions : true
      });
    
    dataObj = JSON.parse(response);
    
    
  // All other objects  
  } else {
  
    dataObj['data'] = [];
    dataObj['data'][0] = {};
    dataObj['data'][0]['values'] = [];
    
    // Loop queryChunks
    for(var i = 0; i < queryChunks.length; i++) {
      
      // Set date range parameters
      var dateRangeSince = queryChunks[i]['since'].toISOString().slice(0, 10);
      var dateRangeUntil = queryChunks[i]['until'].toISOString().slice(0, 10);
      
      
      var dateRange = "&since="+dateRangeSince+"&until="+dateRangeUntil;
      
      // Perform API Request
      var requestUrl = requestEndpoint+query+dateRange+"&access_token="+pageToken;
      
      console.log(requestUrl);
      
      var response = UrlFetchApp.fetch(requestUrl,
                                       {
                                         muteHttpExceptions : true
                                       });
      
      var parseData = JSON.parse(response);      
      
      console.log(JSON.stringify(parseData));
      
      // Merge data object with values from response
      if (parseData['data'].length > 0) {
          dataObj['data'][0]['values'].push(parseData['data'][0]['values']);
      }
      
    }
  }
  
  console.log("DATA_OBJ: %s",JSON.stringify(dataObj));
  
  return dataObj;
}


function getData(request) {  
  
  
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  
  var requestedFields = getFields().forIds(requestedFieldIds);
  
  var postData = graphData(request, "posts?time_increment=1&fields=message,story,created_time,permalink_url,likes.summary(true),comments.summary(true),shares");
  var pageLikesData = graphData(request, "insights/page_fans?fields=values");
  //var pageFansGenderAgeData = graphData(request, "insights/page_fans_gender_age?fields=values");
    
  var outputData = {};
  outputData.posts = postData;
  outputData.page_likes = pageLikesData;
  //outputData.page_fans_gender_age = pageFansGenderAgeData;
  
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
  var valueRows = report['data'][0]['values'][0];
  row["pageLikes"] = report['data'][0]['values'][0][valueRows.length-1]['value'];
  console.log(JSON.stringify(report.data[0]));
  rows[0] = row;
  
  return rows;
  
}

function reportGenderAge(report, field) {
  var rows = [];
  //Define fans per gender (female, male, unknown)
  var fans = {};
  fans['Female'] = 0;
  fans['Male'] = 0;
  fans['Unknown'] = 0;
  
  // Define fans per age
  fans['13-17'] = 0;
  fans['18-24'] = 0;
  fans['25-34'] = 0;
  fans['35-44'] = 0;
  fans['45-54'] = 0;
  fans['55-64'] = 0;
  fans['65+'] = 0;
  
  // Only report last number of fans per gender/age within date range
  // Get gender/age objects
  var results = report.data[0].values[0]['value'];
  
  // Loop all objects
  for (var property in results) {
    if (results.hasOwnProperty(property)) {
      
      // Assign values to gender
      switch (true) {
        case (property.indexOf('F') > -1):
        fans['Female'] += results[property];
        break;
        case (property.indexOf('M') > -1):
        fans['Male'] += results[property];
        break;
        case (property.indexOf('U') > -1):
        fans['Unknown'] += results[property];
        break;
      }
      
      // Assign values to age
      switch (true) {
        case (property.indexOf('13-17') > -1):
          fans['13-17'] += results[property];
          break;
        case (property.indexOf('18-24') > -1):
          fans['18-24'] += results[property];
          break;
        case (property.indexOf('25-34') > -1):
          fans['25-34'] += results[property];
          break;
        case (property.indexOf('35-44') > -1):
          fans['35-44'] += results[property];
          break;
        case (property.indexOf('45-54') > -1):
          fans['45-54'] += results[property];
          break;
        case (property.indexOf('55-64') > -1):
          fans['55-64'] += results[property];
          break;
        case (property.indexOf('65+') > -1):
          fans['65+'] += results[property];
          break;
      }
      
      
      //console.log('%s: %s', property, results[property]);
    }
  }
  
  for (var property in fans) {
    var row = {};
    if (fans.hasOwnProperty(property)) {
      if (property.indexOf('Female') > -1 || property.indexOf('Male') > -1 || property.indexOf('Unknown') > -1) {
        row['pageFansGender'] = property;
        row['pageLikes'] = fans[property];
      } else { 
        row['pageFansAge'] = property;
        row['pageLikes'] = fans[property];
      }
    }
    rows.push(row);
     
   }
  
  return rows;
  
}

function reportToRows(requestedFields, report) {
  rows = [];
  var postsData = reportPosts(report.posts);
  var pageLikesData = reportPageLikes(report.page_likes);
  //var pageFansGenderData = reportGenderAge(report.page_fans_gender_age, 'gender');
    
  // Merge data
  var data = postsData.concat(pageLikesData);
  console.log("MERGED_DATA: %s",JSON.stringify(data));

   
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
      
      // Assign likes data values to rows
      if (field.getId().indexOf('page') > -1) {
        return row.push(data[i]["pageLikes"]);
      }
      
      /*// Assign gender data and pageLikes values to rows
       if (field.getId().indexOf('page') > -1 && typeof data[i]["pageFansGender"] !== 'undefined') {
         switch (field.getId()) {
          case 'pageFansGender':
            return row.push(data[i]["pageFansGender"]);
          case 'pageLikes':
            return row.push(data[i]["pageLikes"]);
         }
       }
       */
      
       /*// Assign age data ans pageLikes values to rows
       else if (field.getId().indexOf('page') > -1 && typeof data[i]["pageFansAge"] !== 'undefined') {
         switch (field.getId()) {
          case 'pageFansAge':
            return row.push(data[i]["pageFansAge"]);
          case 'pageLikes':
            return row.push(data[i]["pageLikes"]);
         }
       }*/
      
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

