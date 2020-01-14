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
      .setId('pageViewsTotal')
      .setName('Page Views')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pageFanAdds')
      .setName('Page Likes')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newDimension()
      .setId('pageFanAddsDate')
      .setName('New Likes Date')
      .setType(types.YEAR_MONTH_DAY);
  
  fields.newMetric()
      .setId('pagePostsReach')
      .setName('Total Posts Reach')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
 
  fields.newMetric()
      .setId('pagePostsEngagement')
      .setName('Total Post Engagement')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newDimension()
      .setId('postDate')
      .setName('Post Date')
      .setType(types.YEAR_MONTH_DAY);
  
  fields.newDimension()
      .setId('postMessage')
      .setName('Post Message')
      .setType(types.TEXT);  
  
  fields.newDimension()
      .setId('postLink')
      .setName('Link to post')
      .setType(types.URL);
  
  fields.newDimension()
       .setId('postMessageHyperLink')
       .setName('Post Message Link')
       .setType(types.HYPERLINK)
       .setFormula('HYPERLINK($postLink,$postMessage)');
  
  fields.newMetric()
      .setId('postReach')
      .setName('Reach on post')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('postReactions')
      .setName('Reactions on post')
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
      .setId('postClicks')
      .setName('Clicks on post')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('postEngagement')
      .setName('Engagement on post')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM)
      .setFormula('$postReactions+$postComments+$postShares+$postClicks');
  
  return fields;
}


function getSchema(request) {  
    var fields = getFields().build();
    return { 'schema': fields };    
}

function getData(request) {   
  
  var nestedData = graphData(request, "?fields=insights.metric(page_views_total, page_fan_adds_unique, page_posts_impressions_unique, page_post_engagements).period(day).since([dateSince]).until([dateUntil]),posts.fields(created_time, message, permalink_url, insights.metric(post_impressions_unique, post_clicks, post_reactions_by_type_total), comments.summary(true), shares).since([dateSince]).until([dateUntil])");
  
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  
  
  var outputData = {};
  var requestedFields = getFields().forIds(requestedFieldIds);
  
  // Perform data request per field
  request.fields.forEach(function(field) {
    var rows = [];
    
        if (field.name == 'pageViewsTotal') {
           outputData.page_views_total = nestedData['page_views_total'];
        }
        if (field.name == 'pageFanAdds' || field.name == 'pageFanAddsDate') {
           outputData.page_fan_adds = nestedData['page_fan_adds_unique'];
        }
        if (field.name == 'pagePostsReach') {
           outputData.page_posts_reach = nestedData['page_posts_impressions_unique'];
        }
        if (field.name == 'pagePostsEngagement') {
           outputData.page_posts_engagement = nestedData['page_post_engagements'];
        }
        if (field.name == 'postDate' || field.name == 'postMessage' || field.name == 'postLink' || field.name == 'postReach' || field.name == 'postReactions') {
          outputData.posts = nestedData['posts'];
        }
    
        
        if (typeof outputData !== 'undefined') {    
          rows = reportToRows(requestedFields, outputData);
          // TODO: parseData.paging.next != undefined
        } else {
          rows = [];
        }
         result = {
            schema: requestedFields.build(),
            rows: rows
          };  
    
  });
  
  return result;  
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
  

// Report all daily reports to rows 
function reportDaily(report, type) {
  var rows = [];
  
  //Loop chunks
  for (var c = 0; c <  report.length; c++) {
  
    var valueRows = report[c];
    
    // Loop report
    for (var i = 0; i < valueRows.length; i++) {
      var row = {};
      
      row[type] = report[c][i]['value'];
      
      if (type == 'pageFanAdds') {
        //Data is reported on day after. Actual date is end_time - 24 hours
        row['pageFanAddsDate'] = formatDate(new Date(new Date(report[c][i]['end_time']).getTime()-86400000));

      }
      
      // Assign all data to rows list
      rows.push(row);
    }
  }

  return rows;
}

// Report posts to rows
function reportPosts(report) {  
  var rows = [];
  
  // Only loop object when it contains data
  if (typeof report.data !== 'undefined' && report.data.length > 0) {
  
    // Loop posts
    for( var i = 0; i < report.data.length; i++) {
      
      // Define empty row object
      var row = {};
      
      //Return date object to ISO formatted string
      row["postDate"] = new Date(report.data[i]['created_time']).toISOString().slice(0, 10);
      
      row["postMessage"] = report.data[i]['message'] || report.data[i]['story'];
      row["postLink"] = report.data[i]['permalink_url'];
      row["postReach"] = report.data[i].insights.data[0].values[0]['value'];
      row["postClicks"] = report.data[i].insights.data[1].values[0]['value'];
      
      // Sum all reactions
      var reactionsCount = 0;
      var reactionsValues = report.data[i].insights.data[2].values[0]['value'];
      for (var property in reactionsValues) {
        reactionsCount += reactionsValues[property];
      }
      row["postReactions"] = reactionsCount;
      
      row["postComments"] = 0;
      // Determine if comments object exist
      if (typeof report.data[i]['comments'] !== 'undefined') {
        row["postComments"] = report.data[i]['comments']['summary']['total_count'];
      }
    
      row["postShares"] = 0;
      // Determine if shares object exist
      if (typeof report.data[i]['shares'] !== 'undefined') {
        row["postShares"] = report.data[i]['shares']['count'];
      }
      
      // Assign all post data to rows list
      rows.push(row);
    }
  }
  
  return rows;
}

function reportToRows(requestedFields, report) {
  var rows = [];
  var data = [];  
  
  if (typeof report.page_views_total !== 'undefined') {
    data = data.concat(reportDaily(report.page_views_total, 'pageViewsTotal'));
  }
  if (typeof report.page_fan_adds !== 'undefined') {
    data = data.concat(reportDaily(report.page_fan_adds, 'pageFanAdds'));
  }   
  if (typeof report.page_posts_reach !== 'undefined') {
    data = data.concat(reportDaily(report.page_posts_reach, 'pagePostsReach'));
  }  
  if (typeof report.page_posts_engagement !== 'undefined') {
    data = data.concat(reportDaily(report.page_posts_engagement, 'pagePostsEngagement'));
  }
  if (typeof report.posts !== 'undefined') {
    data = data.concat(reportPosts(report.posts));
  }   
  
  // Merge data
  for(var i = 0; i < data.length; i++) {
    row = [];    
    requestedFields.asArray().forEach(function (field) {
      return row.push(data[i][field.getId()]);
      
    });
    if (row.length > 0) {
      rows.push({ values: row });
    }
  }
    
  return rows;
    
}


function isAdminUser(){
 var email = Session.getEffectiveUser().getEmail();
  if( email == 'steven@itsnotthatkind.org' || email == 'analyticsintk@gmail.com'){
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
