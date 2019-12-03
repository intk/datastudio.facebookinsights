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
    
    console.log(requestUrl);
    
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
      
      var response = UrlFetchApp.fetch(requestUrl,
                                       {
                                         muteHttpExceptions : true
                                       });
      
      var parseData = JSON.parse(response);      
            
      // Merge data object with values from response
      if (parseData['data'].length > 0) {
          dataObj['data'][0]['values'].push(parseData['data'][0]['values']);
      }
      
    }
  }
  
  console.log(JSON.stringify(dataObj));
  
  
  return dataObj;
}


function getData(request) {  
  
  
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  
  var outputData = {};
  
  // Perform data request per field
  request.fields.forEach(function(field) {
    
    if (field.name == 'pageLikes') {
      outputData.page_likes = graphData(request, "insights/page_fans?fields=values");
    }
  });




  
  var requestedFields = getFields().forIds(requestedFieldIds);
  
  if(typeof outputData !== 'undefined')
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

function reportPageLikes(report) {
  var rows = [];
    
  // Only report last number of page likes within date range
  var row = {};
  var valueRows = report['data'][0]['values'][0];
  row["pageLikes"] = report['data'][0]['values'][0][valueRows.length-1]['value'];
  rows[0] = row;
  
  return rows;
  
}

function reportToRows(requestedFields, report) {
  var rows = [];  
  
  var pageLikesData = reportPageLikes(report.page_likes);
  
  var data = [].concat(pageLikesData);
    
  // Merge data
  for(var i = 0; i < data.length; i++) {
    row = [];    
    requestedFields.asArray().forEach(function (field) {
      
      if (field.getId().indexOf('pageLikes') > -1 && typeof data[i]["pageLikesAddsDate"] === 'undefined') {
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

