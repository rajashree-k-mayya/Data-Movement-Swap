CREATE OR REPLACE FUNCTION public.gettonode_fordatamovement(userid integer, roleid integer, burid integer, locationid integer, nodetypeid integer, searchkey character varying, typeid integer, actid integer)
 RETURNS TABLE(distnid integer, distcode character varying, distname character varying, response jsonb)
 LANGUAGE plpgsql
AS $function$ 
declare distid int4;
BEGIN
insert into assignmentpreelogs(fntext,created) 
select 'gettonode_fordatamovement '||$1::text||'/'||$2::text||'/'||$3::text||'/'||$4::text||'/'||$5::text||'/'||$6::text||'/'||$7::text||'/'||$8::text ,now();

select distributionnodeid into distid from tbldistributionnodes where (distributionnodecode=$6 or distributionnodename=$6)and lastactivityid=$8;

if $7=1 then 
begin
    return query
    select a.distributionnodeid,a.distributionnodecode,a.distributionnodename,'{}'::jsonb from tbldistributionnodes a
    left join etl_midata b on b.distributionnodeid=a.distributionnodeid
    where a.distributionnodeid=distid and a.lastactivityid=$8 and b.distributionnodeid is null;
end;

elsif $7=2 then
begin
    return query

        SELECT r.distnid,r.distcode,r.distname,
               jsonb_agg(
                   jsonb_build_object(
                       'property', p.propertyname,
                       'value', case when (prop->>'categorypropertyallocationid')::int=57 then (prop->>'value')::jsonb->>'meter_Id' 
                                when (prop->>'categorypropertyallocationid')::int=835 then (prop->>'value')::jsonb->>'meter_Id'
                                else prop->>'value' end
                       --'categorypropertyallocationid', (prop->>'categorypropertyallocationid')::int
                   )) AS properties
        from (select r.response,r.distnid,r.distcode,r.distname from tblresponselogs r where r.distnid=distid
        and r.activityid in (72,3) and r.responsestatusid>=0 and r.projectid=$3 and responsestatusid=32 --and r.nextapproverroleid=84--(swap status)
        order by r.responselogid desc limit 1) r
        cross join LATERAL jsonb_array_elements(r.response::jsonb->'propertiesBean') as prop
        inner join tblproperties p on p.propertyid = (prop->>'categorypropertyallocationid')::int and p.componenttypeid not in (13,14,15,16)
        where (prop->>'categorypropertyallocationid')::int not in (1012,1097)
        group by r.distnid, r.distcode, r.distname;

end;
end if;

END;
$function$
