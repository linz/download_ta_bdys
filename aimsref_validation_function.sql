create or replace function admin_bdys_import.validation_meshblock_numcodechar() returns character varying as 
$$
declare
	max_len int := 7;
	mb_sch text := 'admin_bdys_import';
	mb_tbl text := 'statsnz_meshblock';
	mbq1 varchar := 'select count(*) from '|| mb_sch || '.' || mb_tbl ||' where char_length(mb_code)<>'||max_len;
	err_count int := 0;
begin

execute mbq1 into err_count;
return 'VALIDATION: [code length]. ErrorCount = ' || err_count;
	
end;
$$ 
language plpgsql VOLATILE;

--select admin_bdys_import.validation_meshblock_numcodechar();