
-- 3. Inbound Calls per Channel per Day
SELECT triggerDatetime::date AS call_date, ch, COUNT(*) AS calls
FROM RAW.CRM.Infinity_Download
GROUP BY call_date, ch
ORDER BY call_date DESC, ch;

-- 4. Avg. Call Duration by Channel
SELECT ch, AVG(callDuration) AS avg_call_duration_sec
FROM RAW.CRM.Infinity_Download
GROUP BY ch
ORDER BY avg_call_duration_sec DESC;


-- 5. Calls by Call State (Daily)
SELECT triggerDatetime::date AS day, ch, callState, COUNT(*) AS num_calls
FROM RAW.CRM.Infinity_Download
GROUP BY day, ch, callState
ORDER BY day DESC, ch, num_calls DESC;

-- 6. Calls by Campaign and Channel
SELECT campaign, ch, COUNT(*) AS total_calls
FROM RAW.CRM.Infinity_Download
GROUP BY campaign, ch
ORDER BY total_calls DESC;

-- 7. Website Visits by Source (Referrer)
SELECT src, COUNT(*) AS visit_count
FROM RAW.CRM.Infinity_Download
GROUP BY src
ORDER BY visit_count DESC;


--Still to check 


-- 9. Cost per Lead by Channel (if cost info exists in 'res' field)
SELECT ch, 
    SUM(CASE WHEN act='spend' THEN try_cast(res AS float) ELSE 0 END) / NULLIF(SUM(CASE WHEN act='lead' THEN 1 ELSE 0 END), 0) AS cost_per_lead
FROM RAW.CRM.Infinity_Download
GROUP BY ch
ORDER BY cost_per_lead DESC;

-- 10. Calls That Resulted in a Sale (by Source)
SELECT src, COUNT(DISTINCT CASE WHEN act='sale' THEN rowId END) AS sales
FROM RAW.CRM.Infinity_Download
WHERE act IN ('call', 'sale')
GROUP BY src
ORDER BY sales DESC;

-- 11. Conversion Funnel (Impression → Click → Visit → Lead → Call → Sale)
SELECT 
  triggerDatetime::date AS event_date,
  SUM(CASE WHEN act='impression' THEN 1 ELSE 0 END) AS impressions,
  SUM(CASE WHEN act='click' THEN 1 ELSE 0 END) AS clicks,
  SUM(CASE WHEN act='visit' THEN 1 ELSE 0 END) AS visits,
  SUM(CASE WHEN act='lead' THEN 1 ELSE 0 END) AS leads,
  SUM(CASE WHEN act='call' THEN 1 ELSE 0 END) AS calls,
  SUM(CASE WHEN act='sale' THEN 1 ELSE 0 END) AS sales
FROM RAW.CRM.Infinity_Download
GROUP BY event_date
ORDER BY event_date DESC;

-- 12. Product Penetration: % of Sales with Finance and Warranty
SELECT 
  triggerDatetime::date AS sale_date,
  COUNT(DISTINCT rowId) AS sales,
  COUNT(DISTINCT CASE WHEN attr='FINANCE' THEN rowId END) AS finance_sales,
  COUNT(DISTINCT CASE WHEN attr='WARRANTY' THEN rowId END) AS warranty_sales
FROM RAW.CRM.Infinity_Download
WHERE act = 'sale'
GROUP BY sale_date
ORDER BY sale_date DESC;

-- 13. Longest Calls (for audit/training)
SELECT triggerDatetime, ch, callDuration, callRating, operatorRef, callTranscription
FROM RAW.CRM.Infinity_Download
WHERE act = 'call'
ORDER BY callDuration DESC
LIMIT 20;

-- 14. Channel Traffic Over Time (Week)
SELECT DATE_TRUNC('week', triggerDatetime) AS week, ch, COUNT(*) AS event_count
FROM RAW.CRM.Infinity_Download
GROUP BY week, ch
ORDER BY week DESC, event_count DESC;

-- 15. Leads → Appointment Conversion Rate
SELECT
  COUNT(DISTINCT CASE WHEN act='lead' THEN rowId END) AS total_leads,
  COUNT(DISTINCT CASE WHEN act='appointment' THEN rowId END) AS appointments,
  100 * COUNT(DISTINCT CASE WHEN act='appointment' THEN rowId END) / NULLIF(COUNT(DISTINCT CASE WHEN act='lead' THEN rowId END),0) AS appointment_rate_pct
FROM RAW.CRM.Infinity_Download;

-- 16. Calls by Operator and State
SELECT operatorRef, callState, COUNT(*) AS num_calls
FROM RAW.CRM.Infinity_Download
WHERE act = 'call'
GROUP BY operatorRef, callState
ORDER BY num_calls DESC;

-- 17. Attribution: First Touch per Visitor Leading to Sale
SELECT vref, MIN(triggerDatetime) AS first_touch_time, MIN(src) AS first_touch_source
FROM RAW.CRM.Infinity_Download
WHERE act IN ('visit','click','lead')
GROUP BY vref
HAVING MIN(triggerDatetime) IS NOT NULL;

-- 18. Calls by AdGroup, AdRef, Campaign
SELECT adGroup, adRef, campaign, COUNT(*) AS calls
FROM RAW.CRM.Infinity_Download
WHERE act = 'call'
GROUP BY adGroup, adRef, campaign
ORDER BY calls DESC;

-- 19. Daily Error/Issue Monitoring
SELECT triggerDatetime::date AS event_date, COUNT(*) AS total_events,
    SUM(CASE WHEN res='error' OR res='issue' THEN 1 ELSE 0 END) AS error_count
FROM RAW.CRM.Infinity_Download
GROUP BY event_date
ORDER BY event_date DESC;

-- 20. Call Analytics: Positive vs Negative Sentiment (if keyword columns populated)
SELECT 
  AVG(operatorPositiveKeywordCount + contactPositiveKeywordCount) as avg_positive_keywords,
  AVG(operatorNegativeKeywordCount + contactNegativeKeywordCount) as avg_negative_keywords
FROM RAW.CRM.Infinity_Download
WHERE act = 'call' AND callTranscription IS NOT NULL;