drop table if exists ca_adhoc_view.yj_mc_dsct_id;
 
create table ca_adhoc_view.yj_mc_dsct_id as
select 
        customer_link, 
        random() as rand1 
from 
        ( select distinct 
                customer_link 
        from 
                ca_adhoc_view.yj_liveramp_innovid_dcm_response_vw 
        );
 
select 
        count(1), 
        count(distinct customer_link) 
from 
        ca_adhoc_view.yj_mc_dsct_id;
 
drop table if exists ca_adhoc_view.yj_mc_rand_sample;
 
create table ca_adhoc_view.yj_mc_rand_sample as
select 
        t1.*
from 
        ca_adhoc_view.yj_liveramp_innovid_dcm_response_vw as t1, 
        ca_adhoc_view.yj_mc_dsct_id                       as t2
where   t1.customer_link      = t2.customer_link 
        and t1.customer_link != 'UNMATCHED' 
        and t2.rand1          < 0.1;
 
select 
        count(1), 
        count(distinct customer_link) 
from 
        ca_adhoc_view.yj_mc_rand_sample;
        