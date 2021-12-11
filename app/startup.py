import sys
import preprocessing.preglobal as pg
import preprocessing.keynote.s01_fetchkeynotevideos as s01
import preprocessing.keynote.s02_import_existing as s02
import preprocessing.keynote.s03_download_keynote_videos as s03
import preprocessing.keynote.s04_generate_subtitles as s04
import preprocessing.keynote.s04b_download_subtitles as s04b
import preprocessing.keynote.s05_import_subtitles as s05
import preprocessing.keynote.s06_generate_ocr as s06
import preprocessing.keynote.s06b_download_ocr as s06b
import preprocessing.keynote.s08_addmetadata as s08
import preprocessing.keynote.s09_process_ocr as s09

import preprocessing.lob.s01_download_lob as l01
import preprocessing.lob.s02_import_lob as l02
import preprocessing.lob.s03_fill_cache as l03

import frontend.gui as gui

def startup(start_gui=True, do_check=True, update_keynote_list=False, refresh_cache=False):
    if do_check:
        # Step 0:
        pg.start()

        # Step 1: Get Keynote List from Apple Podcast
        if len(pg.get_kn_entries()) == 0 or update_keynote_list:
            assert s01.start()

        # Select relevant keynotes
        selected = {
            '20190910_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20190325_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20181030_AAPL':{'local-time':'10:00:00','time-zone':'et'}, #new york
            '20180912_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20180604_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20180327_AAPL':{'local-time':'10:00:00','time-zone':'ct'}, #chicago
            '20170912_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20170605_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20161027_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20160907_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20160613_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20160321_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20150909_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20150608_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            '20150309_AAPL':{'local-time':'10:00:00','time-zone':'pt'},
            }
        tbl = pg.get_keynote_tbl() 
        tbl.update_many({},{ '$set': { 'selected': None } }, upsert=False)
        for i,v in selected.items():
            v.update({ 'selected': 1  })
            if refresh_cache:
                v.update({ 'lob_cache_filled': None  })
            tbl.update_one({ 'id': i },{ '$set': v }, upsert=False)

        # Step 2: Import already processed files to db
        assert s02.start()

        # Step 3: Do Import Pipeline
        print('step 3')
        assert s03.start()
        print('step 4')
        assert s04.start()
        print('step 4b')
        assert s04b.start()
        print('step 5')
        assert s05.start()
        print('step 6')
        assert s06.start()
        print('step 6b')
        assert s06b.start()
        print('step 8')
        assert s08.start()
        print('step 9')
        assert s09.start()
        print('step l1')
        assert l01.start()
        print('step l2')
        assert l02.start()
        print('step l3')
        assert l03.start()

    if start_gui:
        gui.start_gui()

if __name__ == "__main__":
    if len(sys.argv) == 1:
        startup()
    elif sys.argv[1] == '--refresh_cache':
        startup(refresh_cache=True)
    elif sys.argv[1] == '--nocheck':
        startup(do_check=False)
    elif sys.argv[1] == '--update_keynotes':
        startup(update_keynote_list=True)
    else:
        print('valid options: '+sys.argv[0]+' --nocheck --update_keynotes --refresh_cache')