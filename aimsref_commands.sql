
select admin_bdys_import.import_admin_boundary('meshblock_concordance');
select admin_bdys_import.import_admin_boundary('statsnz_meshblock');
select admin_bdys_import.import_admin_boundary('statsnz_ta');
select admin_bdys_import.import_admin_boundary('nz_locality');
--create ta grid
select public.create_table_polygon_grid('admin_bdys_import','s_territorial_authority','shape',0.05566,0.05566);