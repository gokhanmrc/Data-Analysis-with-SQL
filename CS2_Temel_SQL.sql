SELECT ad_date,
	   campaign_id,
	   SUM(spend) as total_spend,
	   SUM(impressions) as total_impressions,
	   SUM(clicks) as total_clicks,
	   SUM(value) as total_value,
	   CAST(SUM(spend) as FLOAT) / SUM(clicks) as CPC,
	   (CAST(SUM(spend)as FLOAT) / SUM(impressions)) * 1000 as CPM,
	   (CAST(SUM(clicks) as FLOAT) / SUM(impressions)) * 100 as CTR,
	   (CAST(SUM(value) as FLOAT) - SUM(spend)) / SUM(spend) as ROMI
	 FROM Public.facebook_ads_basic_daily
	 where impressions > 0 and clicks > 0 and spend > 0
     group by ad_date, campaign_id
     -- Bonus görev
     order by ROMI desc
     limit 1;
      

	   
		
