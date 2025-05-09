with new_all_tables as (
	select 
		fa1.ad_date,
		'facebook ads' as media_source,
		Coalesce(fa3.campaign_name,'Unknown') as campaign_name,
		coalesce(fa2.adset_name, 'Unknown') as adset_name,
		coalesce(fa1.spend,0) as spend,
		coalesce(fa1.impressions, 0) as impressions,
		coalesce(fa1.reach,0) as reach,
		coalesce(fa1.clicks,0) as clicks,
		coalesce(fa1.leads, 0) as leads,
		coalesce(fa1.value,0) as value,
		coalesce(fa1.url_parameters, 'no utm') as url_parameters	
	from public.facebook_ads_basic_daily as fa1
	left join public.facebook_adset as fa2 on fa1.adset_id = fa2.adset_id
	left join public.facebook_campaign as fa3 on fa1.campaign_id = fa3.campaign_id
	
	union all
	
	select
		go.ad_date,
		'google ads' as media_source,
		Coalesce(go.campaign_name,'Unknown') as campaign_name,
		coalesce(go.adset_name, 'Unknown') as adset_name,
		coalesce(go.spend,0) as spend,
		coalesce(go.impressions, 0) as impressions,
		coalesce(go.reach,0) as reach,
		coalesce(go.clicks,0) as clicks,
		coalesce(go.leads, 0) as leads,
		coalesce(go.value,0) as value,
		coalesce(go.url_parameters, 'no utm') as url_parameters
	from public.google_ads_basic_daily	as go
),
monthly_data as (
	select
	    -- ad_month — reklamın görüntülendiği ayın ilk günü
		date(date_trunc('month' , ad_date)) as ad_month,
		--utm_campaign için regexp_matches fonksiyonu
		CASE 
    		WHEN lower((regexp_match(url_parameters, 'utm_campaign=([^&]*)'))[1]) = 'nan' 
    		THEN NULL
    		ELSE lower((regexp_match(url_parameters, 'utm_campaign=([^&]*)'))[1])
		END AS utm_campaign,
		-- İlgili kampanya için ilgili tarihteki toplam maliyet, gösterim sayısı, tıklama sayısı ve toplam dönüşüm değerleri
		SUM(spend) AS total_cost,
        SUM(impressions) AS number_of_impressions,
        SUM(clicks) AS number_of_clicks,
        SUM(value) AS conversion_value,
        -- İlgili kampanya için ilgili tarihteki CTR, CPC, CPM, ROMI
        case when sum(impressions) != 0 then sum(clicks::numeric)/sum(impressions::numeric)*100 end as ctr,
        case when sum(clicks) != 0 then sum(spend::numeric)/sum(clicks::numeric) end as cpc,
        case when sum(impressions) != 0 then sum(spend::numeric)/sum(impressions::numeric)*1000 end as cpm,
        case when sum(spend) != 0 then (sum(value::numeric)-sum(spend::numeric))/sum(spend::numeric) end as romi
        from new_all_tables
        group by ad_month,utm_campaign
        order by ad_month
),
final_data as (
	select
		*,
		lag(md.ctr) over (partition by md.utm_campaign order by md.ad_month asc) as ctr_1m_ago,
		lag(md.cpc) over (partition by md.utm_campaign order by md.ad_month asc) as cpc_1m_ago,
		lag(md.cpm) over (partition by md.utm_campaign order by md.ad_month asc) as cpm_1m_ago,
		lag(md.romi) over (partition by md.utm_campaign order by md.ad_month asc) as romi_1m_ago
from monthly_data as md
)
select 
	*,
	--CTR ın bir önceki aya göre yüzde olarak farkı.	
	case 
		when fd.ctr_1m_ago != 0 
		then Round((fd.ctr - fd.ctr_1m_ago) 
			/ fd.ctr_1m_ago * 100,2) 
	end as ctr_diff_percent,
	--CPC in bir önceki aya göre yüzde olarak farkı.	
	case 
		when fd.cpc_1m_ago != 0 
		then Round((fd.cpc - fd.cpc_1m_ago) 
			/ fd.cpc_1m_ago * 100,2) 
	end as cpc_diff_percent,
	--CPM in bir önceki aya göre yüzde olarak farkı.	
	case 
		when fd.cpm_1m_ago != 0 
		then Round((fd.cpm - fd.cpm_1m_ago) 
			/ fd.cpm_1m_ago * 100,2) 
	end as cpm_diff_percent,
	--Romi nin bir önceki aya göre yüzde olarak farkı.	
	case 
		when fd.romi_1m_ago != 0 
		then Round((fd.romi - fd.romi_1m_ago) 
			/ fd.romi_1m_ago * 100,2) 
	end as romi_diff_percent
from final_data as fd

        
