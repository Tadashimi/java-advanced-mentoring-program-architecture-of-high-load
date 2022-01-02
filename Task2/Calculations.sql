--Hint: use the functions - countif, uniqExact.
--Use LIMIT key word to get less not important rows.
--Calculate number of events per days
SELECT `date`, event, COUNT(event) 
FROM ads_data1
GROUP BY `date`, event
ORDER BY `date`
LIMIT 10;

--Calculate count of each ad displayed
SELECT ad_id, COUNT(ad_id) 
FROM ads_data1
GROUP BY ad_id
ORDER BY ad_id 
LIMIT  10;

--Calculate number of clicks
SELECT countIf(event == 'click')
FROM ads_data1;

--Calculate  number of unique ads and unique campaigns
SELECT uniqExact(ad_id), uniqExact(campaign_union_id)
FROM ads_data1;

