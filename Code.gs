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

  /*
  ------------------------------------------------------
  DataStudio fields
  ------------------------------------------------------
  */

function getFields() {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;  
  
  fields.newMetric()
      .setId('pageFans')
      .setName('Total Fans')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newDimension()
      .setId('pageFansAddsDate')
      .setName('New Fans Date')
      .setType(types.YEAR_MONTH_DAY);
  
  fields.newMetric()
      .setId('pageFansAdds')
      .setName('New Fans')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pageImpressionsTotal')
      .setName('Total Impressions')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pageImpressionsOrganic')
      .setName('Organic Impressions')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pageImpressionsPaid')
      .setName('Paid Impressions')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pageImpressionsViral')
      .setName('Viral Impressions')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
   fields.newDimension()
      .setId('pageFansAge')
      .setName('Age')
      .setType(types.TEXT);
  
   fields.newMetric()
      .setId('pageFansAgeNumber')
      .setName('Fans per Age')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newDimension()
      .setId('pageFansGender')
      .setName('Gender')
      .setType(types.TEXT);
  
   fields.newMetric()
      .setId('pageFansGenderNumber')
      .setName('Fans per Gender')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
   fields.newMetric()
      .setId('pageConsumptions')
      .setName('Content Clicks')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pagePositiveFeedback')
      .setName('Positive Actions')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pageNegativeFeedback')
      .setName('Negative Actions')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  
    
  return fields;
}


function getSchema(request) {  
    var fields = getFields().build();
    return { 'schema': fields };    
}

function getData(request) {  
  
  
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  
  var outputData = {};
  
  // Perform data request per field
  request.fields.forEach(function(field) {
    
    if (field.name == 'pageFans') {
      outputData.page_fans = graphData(request, "insights/page_fans?fields=values");
    }
    if (field.name == 'pageImpressionsTotal') {
      outputData.page_impressions_total = graphData(request, "insights/page_impressions/day?fields=values");
    }
    if (field.name == 'pageImpressionsOrganic') {
      outputData.page_impressions_organic = graphData(request, "insights/page_impressions_organic/day?fields=values");
    }
    if (field.name == 'pageImpressionsPaid') {
      outputData.page_impressions_paid = graphData(request, "insights/page_impressions_paid/day?fields=values");
    }
    if (field.name == 'pageImpressionsViral') {
      outputData.page_impressions_viral = graphData(request, "insights/page_impressions_viral/day?fields=values");
    }
    if (field.name == 'pageFansAge' || field.name == 'pageFansGender') {
      outputData.page_fans_gender_age = graphData(request, "insights/page_fans_gender_age?fields=values");
    }
    if (field.name == 'pageFansAdds' || field.name == 'pageFansAddsDate') {
      outputData.page_fans_adds = graphData(request, "insights/page_fan_adds?fields=values");
    }
    if (field.name == 'pageConsumptions') {
      outputData.page_consumptions = graphData(request, "insights/page_consumptions/day?fields=values");
    }
    if (field.name == 'pagePositiveFeedback') {
      outputData.page_positive_feedback = graphData(request, "insights/page_positive_feedback_by_type/day?fields=values");
    }
    if (field.name == 'pageNegativeFeedback') {
      outputData.page_negative_feedback = graphData(request, "insights/page_negative_feedback/day?fields=values");
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

function reportPageFans(report) {
  var rows = [];
    
  // Only report last number of page likes within date range
  var row = {};
  var valueRows = report['data'][0]['values'][0];
  row["pageFans"] = report['data'][0]['values'][0][valueRows.length-1]['value'];
  rows[0] = row;
  
  return rows;
  
}

// Report all daily reports to rows 
function reportDaily(report, type) {
  var rows = [];
  
  var valueRows = report['data'][0]['values'][0];
  
  // Loop report
  for (var i = 0; i < valueRows.length; i++) {
    var row = {};
    
    row[type] = report['data'][0]['values'][0][i]['value'];
    
    // Assign all data to rows list
    rows.push(row);
  }
  
  return rows;
}

function reportGenderAge(report) {
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
  var results = report.data[0].values[0][report.data[0].values[0].length-1]['value'];
  console.info(results);
  
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
      
    }
  }
  
  for (var property in fans) {
    var row = {};
    if (fans.hasOwnProperty(property)) {
      if (property.indexOf('Female') > -1 || property.indexOf('Male') > -1 || property.indexOf('Unknown') > -1) {
        row['pageFansGender'] = property;
        row['pageFansGenderNumber'] = fans[property];
      } else { 
        row['pageFansAge'] = property;
        row['pageFansAgeNumber'] = fans[property];
      }
    }
    rows.push(row);
     
   }
  
  console.log(rows);
  
  return rows;
  
}

//Format date object to YYMMDD
function formatDate(date) {
    var d = new Date(date),
        month = '' + (d.getMonth() + 1),
        day = '' + d.getDate(),
        year = d.getFullYear();

    if (month.length < 2) 
        month = '0' + month;
    if (day.length < 2) 
        day = '0' + day;

    return [year, month, day].join('');
}

function reportPageFansAdds(report) {
  var rows = [];
    
  // Only report last number of page likes within date range
  var valueRows = report['data'][0]['values'][0];
  
  for (var i = 0; i < valueRows.length; i++) {
    var row = {};
    row["pageFansAdds"] = report['data'][0]['values'][0][i]['value'];
    // Data is reported one day before. end_time minus 1 day in milliseconds brings the right date
    row["pageFansAddsDate"] = formatDate(new Date(new Date(report['data'][0]['values'][0][i]['end_time']).getTime()-86400000));

    console.log("pageFansAddsDate %s", new Date(new Date(report['data'][0]['values'][0][i]['end_time']).getTime()-86400000).toISOString().slice(0, 10));
    
    // Assign all data to rows list
    rows.push(row);
  }
  
  console.log(rows);

  return rows;
  
}

// Report positive feedback to rows
function reportPositiveFeedback(report, type) {
  var rows = [];
  
  var valueRows = report['data'][0]['values'][0];
  
  // Loop report
  for (var i = 0; i < valueRows.length; i++) {
    var row = {};
    var count = 0;
    
     // Determine if field exists. Count amount of positive actions
    var feedbackTypes = ['answer', 'claim', 'comment', 'like', 'link', 'other', 'rsvp'];

    for (var property in feedbackTypes) {
       if (typeof report['data'][0]['values'][0][i]['value'][feedbackTypes[property]] !== 'undefined') {
         count += report['data'][0]['values'][0][i]['value'][feedbackTypes[property]];
       }
    }
   
    row[type] = count;
    
    // Assign all data to rows list
    rows.push(row);
  }
  
  return rows;
}

function reportToRows(requestedFields, report) {
  var rows = [];
  var data = [];  
  
  if (typeof report.page_fans !== 'undefined') {
    data = data.concat(reportPageFans(report.page_fans));
  }
  if (typeof report.page_impressions_total !== 'undefined') {
    data = reportDaily(report.page_impressions_total, 'pageImpressionsTotal');
  }  
  if (typeof report.page_impressions_organic !== 'undefined') {
    data = reportDaily(report.page_impressions_organic, 'pageImpressionsOrganic');
  }
  if (typeof report.page_impressions_paid !== 'undefined') {
    data = reportDaily(report.page_impressions_paid, 'pageImpressionsPaid');
  }  
  if (typeof report.page_impressions_viral !== 'undefined') {
    data = reportDaily(report.page_impressions_viral, 'pageImpressionsViral');
  }
  if (typeof report.page_fans_gender_age !== 'undefined') {
    data = reportGenderAge(report.page_fans_gender_age);
  }
  if (typeof report.page_fans_adds !== 'undefined') {
    data = reportPageFansAdds(report.page_fans_adds);
  }
  if (typeof report.page_consumptions !== 'undefined') {
    data = reportDaily(report.page_consumptions, 'pageConsumptions');
  }  
  if (typeof report.page_positive_feedback !== 'undefined') {
    data = reportPositiveFeedback(report.page_positive_feedback, 'pagePositiveFeedback');
  }
  if (typeof report.page_negative_feedback !== 'undefined') {
    data = reportDaily(report.page_negative_feedback, 'pageNegativeFeedback');
  } 
  
    
  // Merge data
  for(var i = 0; i < data.length; i++) {
    row = [];    
    requestedFields.asArray().forEach(function (field) {
  
         switch (field.getId()) {
           case 'pageFans':
              return row.push(data[i]["pageFans"]);
           case 'pageFansAdds':
              return row.push(data[i]["pageFansAdds"]);
           case 'pageFansAddsDate':
              return row.push(data[i]["pageFansAddsDate"]);
           case 'pageImpressionsOrganic':
             return row.push(data[i]["pageImpressionsOrganic"]);
           case 'pageImpressionsPaid':
             return row.push(data[i]["pageImpressionsPaid"]);
           case 'pageImpressionsViral':
             return row.push(data[i]["pageImpressionsViral"]);
           case 'pageImpressionsTotal':
             return row.push(data[i]["pageImpressionsTotal"]);
           case 'pageConsumptions':
               return row.push(data[i]["pageConsumptions"]);
           case 'pagePositiveFeedback':
               return row.push(data[i]["pagePositiveFeedback"]);
           case 'pageNegativeFeedback':
               return row.push(data[i]["pageNegativeFeedback"]);
           case 'pageFansAge':
                return row.push(data[i]["pageFansAge"]);
           case 'pageFansAgeNumber':
               return row.push(data[i]["pageFansAgeNumber"]);
           case 'pageFansGender':
             if (typeof data[i]["pageFansGender"] !== 'undefined') {
               return row.push(data[i]["pageFansGender"]);
             }
           case 'pageFansGenderNumber':
             if (typeof data[i]["pageFansGenderNumber"] !== 'undefined') {
               return row.push(data[i]["pageFansGenderNumber"]);
             }

           
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

