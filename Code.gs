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
      .setId('pageLikesTotal')
      .setName('Total Page Likes')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pageViewsTotal')
      .setName('Page Views')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pageNewLikes')
      .setName('New Likes')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newDimension()
      .setId('pageNewLikesDate')
      .setName('New Likes Date')
      .setType(types.YEAR_MONTH);
  
  fields.newDimension()
      .setId('pageLikesGender')
      .setName('Gender')
      .setType(types.TEXT);

  fields.newMetric()
      .setId('pageLikesGenderNumber')
      .setName('Likes per Gender')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newDimension()
      .setId('pageLikesAge')
      .setName('Age')
      .setType(types.TEXT);

  fields.newMetric()
      .setId('pageLikesAgeNumber')
      .setName('Likes per Age')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newDimension()
      .setId('pageAudienceLanguage')
      .setName('Language')
      .setType(types.TEXT);
  
  fields.newMetric()
      .setId('pageAudienceLanguageLikes')
      .setName('Likes per language')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newDimension()
      .setId('pageLikesSource')
      .setName('Source of Likes')
      .setType(types.TEXT);
  
  fields.newMetric()
      .setId('pageLikesSourceNumber')
      .setName('Likes by Source')
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
      .setId('postImpressions')
      .setName('Impressions')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('postEngagement')
      .setName('Engagement')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pagePostsImpressions')
      .setName('Total Impressions')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  
  fields.newMetric()
      .setId('pagePostsEngagement')
      .setName('Total Engagement')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);

  
  return fields;
}


function getSchema(request) {  
    var fields = getFields().build();
    return { 'schema': fields };    
}

function getData(request) {   
  
  var nestedData = graphData(request, "?fields=insights.metric(page_fans, page_views_total, page_fan_adds, page_fans_gender_age, page_fans_locale, page_posts_impressions, page_post_engagements, page_fans_by_like_source).since([dateSince]).until([dateUntil]),posts.fields(created_time, message, permalink_url, insights.metric(post_impressions, post_engaged_users)).since([dateSince]).until([dateUntil])");
  
  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  
  
  var outputData = {};
  var requestedFields = getFields().forIds(requestedFieldIds);
  
  // Perform data request per field
  request.fields.forEach(function(field) {
    var rows = [];
    
        if (field.name == 'pageLikesTotal') {
           outputData.page_likes_total = nestedData['page_fans'];
        }
        if (field.name == 'pageViewsTotal') {
           outputData.page_views_total = nestedData['page_views_total'];
        }
        if (field.name == 'pageNewLikes') {
           outputData.page_new_likes = nestedData['page_fan_adds'];
        }
        if (field.name == 'pageLikesAge' || field.name == 'pageLikesGender') {
          outputData.page_likes_gender_age = nestedData['page_fans_gender_age'];
        }
        if (field.name == 'pageAudienceLanguage') {
          outputData.page_audience_language = nestedData['page_fans_locale'];
        }
        if (field.name == 'postDate') {
          outputData.posts = nestedData['posts'];
        }
        if (field.name == 'pagePostsImpressions') {
          outputData.page_posts_impressions = nestedData['page_posts_impressions'];
        }
        if (field.name == 'pagePostsEngagement') {
          outputData.page_posts_engagement = nestedData['page_post_engagements'];
        }
        if (field.name == 'pageLikesSource') {
          outputData.page_likes_source = nestedData['page_fans_by_like_source'];
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


function reportToRows(requestedFields, report) {
  var rows = [];
  var data = [];  
  
  if (typeof report.page_likes_total !== 'undefined') {
    data = data.concat(reportPageLikesTotal(report.page_likes_total, 'pageLikesTotal'));
  }
  if (typeof report.page_views_total !== 'undefined') {
    data = data.concat(reportMetric(report.page_views_total, 'pageViewsTotal'));
  }
  if (typeof report.page_new_likes !== 'undefined') {
    data = data.concat(reportMetric(report.page_new_likes, 'pageNewLikes'));
  }
  if (typeof report.page_likes_gender_age !== 'undefined') {
    data = data.concat(reportGenderAge(report.page_likes_gender_age));
  }  
  if (typeof report.page_audience_language !== 'undefined') {
    data = data.concat(reportPageLikesLocale(report.page_audience_language, 'pageAudienceLanguage'));
  }  
  if (typeof report.posts !== 'undefined') {
    data = data.concat(reportPosts(report.posts));
  }  
  if (typeof report.page_posts_impressions !== 'undefined') {
    data = data.concat(reportMetric(report.page_posts_impressions, 'pagePostsImpressions'));
  }
  if (typeof report.page_posts_engagement !== 'undefined') {
    data = data.concat(reportMetric(report.page_posts_engagement, 'pagePostsEngagement'));
  }
  if (typeof report.page_likes_source !== 'undefined') {
    data = data.concat(reportPageLikesSource(report.page_likes_source, 'pageLikesSource'));
  }
  
  // Merge data
  for(var i = 0; i < data.length; i++) {
    row = [];    
    requestedFields.asArray().forEach(function (field) {
      
      //When field is undefined, don't create empty row
      if (typeof data[i][field.getId()] !== 'undefined') {
        return row.push(data[i][field.getId()]);
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