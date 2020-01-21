// Total page likes is a cumulative number. Get the page likes on the last day of the date range.
function reportPageLikesTotal(report, type) {
  var rows = [];
  report = report.day;
  
  var lastObject = report[report.length-1];
  var row = {};
  row[type] =  lastObject[lastObject.length-1]['value'];
  
  rows[0] = row;
  return rows;
}

function reportMetric(report, type) {
  var rows = [];
  
  report = report.day;
    
  //Loop chunks
  for (var c = 0; c <  report.length; c++) {
  
    var valueRows = report[c];
    
    // Loop report
    for (var i = 0; i < valueRows.length; i++) {
      var row = {};
      
      row[type] = report[c][i]['value'];
     
      // Assign all data to rows list
      rows.push(row);
      
    }
  }

  return rows;
}

function reportGenderAge(report) {
  var rows = [];
  
  report = report.day;
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
  var lastObject = report[report.length-1]
  var results = lastObject[lastObject.length-1]['value'];

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
        row['pageLikesGender'] = property;
        row['pageLikesGenderNumber'] = fans[property];
      } else { 
        row['pageLikesAge'] = property;
        row['pageLikesAgeNumber'] = fans[property];
      }
    }
    rows.push(row);

   }

  return rows;

}

//Report language of page fnas
function reportPageLikesLocale(report, type) {
  var rows = [];
  report = report.day;
  
  var lastObject = report[report.length-1]
  var results = lastObject[lastObject.length-1]['value'];
  
         for (var property in results) {
           var row = {};
           row[type] = property;
           row['pageAudienceLanguageLikes'] = results[property];
           rows.push(row);
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
      
      // Exclude events without message
      if (typeof report.data[i]['message'] !== 'undefined') {
      
        // Define empty row object
        var row = {};
        
        //Return date object to ISO formatted string
        row["postDate"] = new Date(report.data[i]['created_time']).toISOString().slice(0, 10);
        
        row["postMessage"] = report.data[i]['message'] || report.data[i]['story'];
        row["postLink"] = report.data[i]['permalink_url'];
        row["postReach"] = report.data[i].insights.data[0].values[0]['value'];
        
        // Assign all post data to rows list
        rows.push(row);
      }
    }
  }
  
  return rows;
}