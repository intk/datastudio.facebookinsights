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
      .setId('date')
      .setName('Date')
      .setType(types.YEAR_MONTH_DAY);
  
  fields.newDimension()
      .setId('id')
      .setName('Id')
      .setType(types.TEXT);  

  fields.newDimension()
      .setId('post')
      .setName('Post')
      .setType(types.TEXT);  

  fields.newDimension()
      .setId('link')
      .setName('Link')
      .setType(types.TEXT);    
  
  fields.newMetric()
      .setId('likes')
      .setName('Likes')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
  return fields;
}


function getSchema(request) {  
    var fields = getFields().build();
    return { 'schema': fields };    
}


function getData(request) {    
  /*var cache = CacheService.getScriptCache();    
  var request_hash = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, JSON.stringify(request)); 
  
  var cached_result = JSON.parse(cache.get(request_hash));
  
  if( cached_result != null ){
    return cached_result;
  }*/
  
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  
  var requestedFields = getFields().forIds(requestedFieldIds);    

  var startDate = new Date(request['dateRange'].startDate);
  var endDate = new Date(request['dateRange'].endDate);

  var pageId = request.configParams['page_id'];
  
  var requestEndpoint = "https://graph.facebook.com/v5.0/"+pageId+"/posts?"
  
  var timeRange = "{'since':'" + startDate.toISOString().slice(0, 10) + "','until':'" + endDate.toISOString().slice(0, 10) + "'}";

  var requestUrl = requestEndpoint += "time_increment=1";
  
  requestUrl += "&fields=message,story,created_time,permalink_url,likes.summary(true)";
  requestUrl += "&time_range=" + encodeURIComponent(timeRange);
  
  console.log(requestUrl);
       
  var response = UrlFetchApp.fetch(requestUrl,
      {
        headers: { 'Authorization': 'Bearer ' + getOAuthService().getAccessToken() },
        muteHttpExceptions : true
      });

  var parseData = JSON.parse(response)

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
  console.info(result)
  return result;  
}


function reportToRows(requestedFields, report) {  
  rows = [];
  
  for( var i = 0; i < report.data.length; i++){
    var row = [];
    var id = report.data[i]['id'];
    
    //Return date object to ISO formatted string
    var date = new Date(report.data[i]['created_time']).toISOString().slice(0, 10);
    console.log(date);
    var post = report.data[i]['message'] || report.data[i]['story'];
    var link = report.data[i]['permalink_url'];
    var likes = 0;
    
    // Determine if likes object exist
    if (typeof report.data[i]['likes'] !== 'undefined') {
      likes = report.data[i]['likes']['summary']['total_count'];
    }
        
    requestedFields.asArray().forEach(function (field) {
      switch (field.getId()) {
          case 'date':
            return row.push(date.replace(/-/g,''));
          case 'id':
            return row.push(id);
          case 'post':
            return row.push(post);
          case 'link':
            return row.push(link);
          case 'likes':
            return row.push(likes);
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
    .setScope('manage_pages');
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

