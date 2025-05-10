import logging

import kioskdatetimelib
import kioskstdlib
from kiosksqldb import KioskSQLDb


# time zone relevance: unless there is a date_entered we set the current server's time as the modified field and time zone
#  and the _ww field to be US/Eastern. If there is a date entered we set the given date as modified and don't care about time zones.
def import_rliim_cm_for_new_loci(commit=True):

    sql_insert = """
        insert into collected_material(uid_locus, type, description, quantity, isobject, date, storage,
                               status_todo, status_done, dearregistrar, created, modified, modified_tz, modified_ww,
                               modified_by, arch_domain, arch_context,
                               cm_type)
        select locus.uid locus_uid, 
               rliim.type, 
               concat_ws(' ', rliim.description, rliim.sf_description, case when rliim.sf_diameter_perf_mm is null then null else 'diameter perf.: ' || rliim.sf_diameter_perf_mm end),
               rliim.count,  
               CASE WHEN rliim.cm_type = 'small_find' THEN 1 ELSE 0 END as isobject,
               coalesce(rliim.sf_date_excavated, rliim.date_excavated), 
               rliim.location, 
               case when rliim.photographed THEN 'P' ELSE '' END, 
               case when rliim.photographed THEN 'P' ELSE '' END, 
               'imported by rliim kiosk bridge', 
               coalesce(coalesce(rliim.sf_date_entered, rliim.date_entered), now() at time zone 'US/Eastern'), 
               case when coalesce(rliim.sf_date_entered, rliim.date_entered) is null then now() else coalesce(rliim.sf_date_entered, rliim.date_entered) end, 
               case when coalesce(rliim.sf_date_entered, rliim.date_entered) is null then 99401495 else 98284531 end, 
               case when coalesce(rliim.sf_date_entered, rliim.date_entered) is null then now() at time zone 'US/Eastern' else coalesce(rliim.sf_date_entered, rliim.date_entered) end, 
               'admin', rliim.arch_domain, 
               rliim.arch_context, 
               rliim.cm_type 
        from rliim_cm_import rliim
             inner join locus on rliim.locus = locus.arch_context
             left outer join collected_material on rliim.arch_context = collected_material.arch_context
        where collected_material.arch_context is null and rliim.locus is not null;
    """

    sql_add_small_finds = """
        insert into small_find(uid_cm, material,
                               length, width, thickness, weight, height, diameter, id_registrar, created,
                               modified, modified_tz, modified_ww, modified_by)
        select cm.uid, rliim.sf_type, rliim.sf_length_mm, rliim.sf_width_mm, rliim.sf_thickness_mm, 
               rliim.sf_weight, 
               rliim.sf_height_mm, rliim.sf_diameter_mm, 'admin', 
               coalesce(rliim.sf_date_entered, now() at time zone 'US/Eastern'), 
               case when rliim.sf_date_entered is null then now() else rliim.sf_date_entered end, 
               case when rliim.sf_date_entered is null then 99401495 else 98284531 end, 
               coalesce(rliim.sf_date_entered, now() at time zone 'US/Eastern'), 
               'admin' 
               from rliim_cm_import rliim
            inner join collected_material cm on rliim.arch_context = cm.arch_context
            where cm.cm_type = 'small_find' and cm.uid not in (select uid_cm from small_find)
    """

    sql_analyze = f"""
        select unit.arch_context, count(distinct locus.arch_context) loci, count(rliim.arch_context) cms
        from rliim_cm_import rliim
                 inner join locus on rliim.arch_domain = locus.arch_context
                 inner join unit on locus.uid_unit = unit.uid
                 left outer join collected_material on rliim.arch_context = collected_material.arch_context
        where collected_material.arch_context is null group by unit.arch_context;
    """

    try:
        sp = KioskSQLDb.begin_savepoint()
    except BaseException as e:
        logging.error(f"rliim2kioskimport.import_rliim_cm_for_new_loci: Exception when beginning savepoint: {repr(e)}")
        return False

    try:
        new_cms = KioskSQLDb.get_records(sql_analyze, raise_exception=True)
        for r in new_cms:
            logging.info(f"RLIIM Collected Material Import: Trench {r[0]} gets {r[2]} new cms in {r[1]} loci.")

        row_count = KioskSQLDb.execute(sql_insert, commit=False)
        if row_count:
            logging.info(f"RLIIM Collected Material Import: Import of {row_count} collected materials successful")
        else:
            logging.info(f"RLIIM Collected Material Import: Note that no new collected materials were added.")
        row_count = KioskSQLDb.execute(sql_add_small_finds, commit=False)
        if row_count:
            logging.info(f"RLIIM Collected Material Import successful: Added {row_count} small find records")
        else:
            logging.info(f"RLIIM Collected Material Import successful.")

        KioskSQLDb.commit_savepoint(sp)
        if commit:
            KioskSQLDb.commit()
        return True
    except BaseException as e:
        logging.error(f"anc2kioskimport.import_rliim_cm_for_new_loci: {repr(e)}")
        KioskSQLDb.rollback_savepoint(sp)
        if commit:
            KioskSQLDb.rollback()

    return False


