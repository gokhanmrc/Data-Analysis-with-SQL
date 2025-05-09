--Bonus görev için geçici fonksiyon oluşturma
CREATE FUNCTION pg_temp.extract_utm_campaign_gkn(url_parameters TEXT) RETURNS TEXT AS $$
BEGIN
    RETURN (
        CASE 
            WHEN lower((SELECT (regexp_matches(url_parameters, 'utm_campaign=([^&]*)'))[1])) = 'nan' THEN NULL
            ELSE lower((SELECT (regexp_matches(url_parameters, 'utm_campaign=([^&]*)'))[1]))
        END
    );
END;
$$ LANGUAGE plpgsql;

/* 
Birleştirilecek tablolar
fa1 = public.facebook_ads_basic_daily tablosu => campaign name ve adset_name haric tüm bilgiler mevcut
fa2 =  public.facebook_adset
fa3 = public.facebook_campaign
go = public.google_ads_basic_daily
*/

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
) 
	select
		-- ad_date - reklam gösterim tarihi
		ad_date,
		-- utm_campaign - utm_campaign parametresinin utm_parameters alanındaki ifadeyi karşılayan değer
		--Bonus görev = geçici fonksiyon kullanma
		extract_utm_campaign_gkn(url_parameters) as utm_campaign,
		-- İlgili kampanya için ilgili tarihteki toplam maliyet, gösterim sayısı, tıklama sayısı ve toplam dönüşüm değerleri
		SUM(spend) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        -- İlgili kampanya için ilgili tarihteki CTR, CPC, CPM, ROMI
        case when sum(impressions) != 0 then sum(clicks::numeric)/sum(impressions::numeric)*100 end as ctr,
        case when sum(clicks) != 0 then sum(spend::numeric)/sum(clicks::numeric) end as cpc,
        case when sum(impressions) != 0 then sum(spend::numeric)/sum(impressions::numeric)*1000 end as cpm,
        case when sum(spend) != 0 then (sum(value::numeric)-sum(spend::numeric))/sum(spend::numeric) end as romi
        from new_all_tables
        group by ad_date,utm_campaign
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	