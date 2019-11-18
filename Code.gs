var cc = DataStudioApp.createCommunityConnector();

function getConfig() {
  var config = cc.getConfig();

  config.newInfo()
      .setId('instructions')
  .setText('Please enter the configuration data for your Facebook connector');

  config.newTextInput()
      .setId('ads_account_id')
      .setName('Enter your Facebook Ads Account Id')
      .setHelpText('')  
      .setPlaceholder('1046444455411879')
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
      .setId('campaign')
      .setName('Campaign')
      .setType(types.TEXT);  

  fields.newDimension()
      .setId('adset')
      .setName('Adset')
      .setType(types.TEXT);    
  
  /*fields.newDimension()
      .setId('gender')
      .setName('Gender')
      .setType(types.TEXT);  
  
  fields.newDimension()
      .setId('age')
      .setName('Age')
      .setType(types.TEXT);  
  
  fields.newDimension()
      .setId('country')
      .setName('Country')
      .setType(types.TEXT); */
  
  fields.newMetric()
      .setId('impressions')
      .setName('Impressions')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('clicks')
      .setName('Clicks')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('spend')
      .setName('Spend')
      .setType(types.CURRENCY_EUR)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('transactions')
      .setName('Transactions')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  fields.newMetric()
      .setId('revenue')
      .setName('Revenue')
      .setType(types.CURRENCY_EUR)
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

  var adsAccountId = request.configParams['ads_account_id'];
  
  var requestEndpoint = "https://graph.facebook.com/v5.0/act_"+adsAccountId+"/insights?"
  
  var timeRange = "{'since':'" + startDate.toISOString().slice(0, 10) + "','until':'" + endDate.toISOString().slice(0, 10) + "'}";

  var requestUrl = requestEndpoint += "time_increment=1";
  
  requestUrl += "&limit=10000";
  requestUrl += "&level=adset";
  //requestUrl += "&breakdowns=['country']";
  requestUrl += "&fields=campaign_name,adset_name,impressions,clicks,spend,actions,action_values";
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
    
    var campaign = report.data[i]['campaign_name'];
    var adset = report.data[i]['adset_name'];
    var impressions = report.data[i]['impressions'];
    var spend = report.data[i]['spend'];
    var clicks = report.data[i]['clicks'];
    var date = report.data[i]['date_start'];
    //var gender = report.data[i]['gender'];
    //var age = report.data[i]['age'];
    //var country = report.data[i]['country'];
    
    var transactions = 0;
    var revenue = 0;
    
    if( 'actions' in report.data[i] ){    
      for(var j = 0; j < report.data[i]['actions'].length; j++ ){
        if( report.data[i]['actions'][j]['action_type'] == 'offsite_conversion.fb_pixel_purchase' ){
          transactions = report.data[i]['actions'][j]['value'];
          break;
        }
      }
    }

    if( 'action_values' in report.data[i] ){    
      for(var j = 0; j < report.data[i]['action_values'].length; j++ ){
        if( report.data[i]['action_values'][j]['action_type'] == 'offsite_conversion.fb_pixel_purchase' ){
          revenue = report.data[i]['action_values'][j]['value'];
          break;
        }
      }
    }
        
    requestedFields.asArray().forEach(function (field) {
      switch (field.getId()) {
          case 'date':
            return row.push(date.replace(/-/g,''));
          case 'campaign':
            return row.push(campaign);
          case 'impressions':
            return row.push(impressions);
          case 'clicks':
            return row.push(clicks);
          case 'spend':
            return row.push(spend);
          case 'transactions':
            return row.push(transactions);
          case 'revenue':
            return row.push(revenue);
          case 'adset':
            return row.push(adset);
      }
    });
    
    rows.push({ values: row });
  }
  return rows;
}          


function isAdminUser(){
 var email = Session.getEffectiveUser().getEmail();
  if( email == 'bjoern.stickler@reprisedigital.com' ){
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
    .setScope('ads_read');
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

