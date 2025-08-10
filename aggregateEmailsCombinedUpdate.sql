CREATE OR REPLACE FUNCTION `${GCP_PROJECT_ID}.in_udf.aggregateEmailsCombinedUpdate`(old_json STRING, new_emails ARRAY<STRING>, new_dates ARRAY<STRING>) RETURNS JSON LANGUAGE js
OPTIONS (description="Merge old_json + new_emails/new_dates into one JSON of Primary..Email_n, skipping duplicate emails") AS R"""
// track seen emails
  var seen = {};

  // helper to push only non-empty, not-yet-seen emails
  function pushPair(arr, email, dateStr) {
    if (email && !seen[email]) {
      seen[email] = true;
      arr.push({
        email: email,
        // parse date string to JS Date; BigQuery will handle nulls and conversions
        date: dateStr ? new Date(dateStr) : null
      });
    }
  }

  var pairs = [];

  // 1) Rehydrate old JSON if present
  if (old_json) {
    try {
      var oldObj = JSON.parse(old_json);
      for (var key in oldObj) {
        if (!key.endsWith("Date")) {
          pushPair(pairs, oldObj[key], null);
        }
      }
    } catch (e) {
      // malformed old_json → drop it
      pairs = [];
    }
  }

  // 2) Append all the new batch
  if (new_emails && new_dates && new_emails.length === new_dates.length) {
    for (var i = 0; i < new_emails.length; i++) {
      pushPair(pairs, new_emails[i], new_dates[i]);
    }
  }

  // 3) If there were NO new emails at all, just return old_json verbatim as parsed JSON
  if (old_json && (!new_emails || new_emails.length === 0)) {
    try {
      return JSON.parse(old_json);
    } catch (e) {
      // In case the old_json was malformed, fall back to an empty object
      return {};
    }
  }

  // 4) Sort by date descending; null dates (old items) go last,
  //    ties among nulls keep original insertion order.
  pairs.sort(function(a, b) {
    if (a.date === null && b.date === null) return 0;
    if (a.date === null) return 1;
    if (b.date === null) return -1;
    return b.date - a.date;
  });

  // 5) Build the output object with only email keys
  var out = {};
  for (var i = 0; i < pairs.length; i++) {
    var label =
      (i === 0 ? "Primary" :
       i === 1 ? "Secondary" :
       i === 2 ? "Tertiary" :
                 "Email_" + (i + 1));
    out[label] = pairs[i].email;
  }

// If nothing ever got pushed, return SQL NULL instead of an empty object
  if (Object.keys(out).length === 0) {
    return null;
  }
  return out;
""";