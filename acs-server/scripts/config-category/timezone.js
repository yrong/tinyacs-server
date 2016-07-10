/*
npm install underscore
npm install sprintf-js

node timezone.js
=> timezone.json
*/
var _ = require('underscore');
var fs = require('fs');
var util = require('util');
var sprintf = require("sprintf-js").sprintf;
var tzData = require('./timezone-data.js');
var tzCategory = require('./timezone-abstract.json');
var tzFilePath = './timezone.json';

var tzValueEnums = [], tzImplies = {}, tzIndex;

_.each(tzData, function(tz, i) {
  var sign = tz.offset >= 0 ? '+' : '-';
  var offset = Math.abs(tz.offset);

  var offsetHour = parseInt(offset / 60);
  var offsetMinute = parseInt(offset % 60);
  var tzOffset = sign + sprintf("%02d:%02d", offsetHour, offsetMinute);
  var tzValue = 'Timezone_' + sprintf("%03d", i);

  tzValueEnums.push({
    "value": tzValue,
    "displayName": tz.text
  });

  tzImplies[tzValue] = {
    "TzOffset": tzOffset,
    "TzName": tz.value
  }
});

tzIndex = _.findIndex(tzCategory.parameters, {
  "name": "Tz"
});

if (tzIndex < 0) {
  console.log('Missing parameter \'Tz\' in timezone-abstract.json');
  process.exit(1);
}

tzCategory.parameters[tzIndex].valueEnums = tzValueEnums;
tzCategory.parameters[tzIndex].implies = tzImplies;

if (fs.existsSync(tzFilePath)) {
  fs.truncateSync(tzFilePath, 0);
}
fs.writeFileSync(tzFilePath, JSON.stringify(tzCategory, null, 2));

console.log('Generated timezone.json');
