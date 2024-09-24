from servicex.app.transforms import LogLevel, TimeFrame
from servicex.app.transforms import add_query, select_time, create_kibana_link_parameters


def test_add_query():
    key = "abc"
    value = "123-345-567"
    query = "(query:(match_phrase:(abc:'123-345-567')))"
    assert add_query(key, value) == query

    key = "requestId"
    value = "d2ede739-9779-4075-95b1-0c7fae1de408"
    query = "(query:(match_phrase:(requestId:'d2ede739-9779-4075-95b1-0c7fae1de408')))"
    assert add_query(key, value) == query


def test_select_time():
    time_frame = TimeFrame.week
    time_filter = "time:(from:now%2Fw,to:now%2Fw)"
    assert time_filter == select_time(time_frame)

    time_frame = "month"
    time_filter = "time:(from:now-30d%2Fd,to:now)"
    assert time_filter == select_time(time_frame)

    time_frame = "daY"
    time_filter = "time:(from:now%2Fd,to:now%2Fd)"
    assert time_filter == select_time(time_frame)


def test_create_kibana_link_parameters():
    initial_log_url = "https://atlas-kibana.mwt2.org:5601/s/servicex/app"\
                      "/dashboards?auth_provider_hint=anonymous1#/view/"\
                      "2d2b3b40-f34e-11ed-a6d8-9f6a16cd6d78?embed=true&_g=()"\
                      "&show-time-filter=true&hide-filter-bar=true"
    transform_id = "d2ede739-9779-4075-95b1-0c7fae1de408"
    log_level = LogLevel.error
    time_frame = TimeFrame.day
    final_url = "https://atlas-kibana.mwt2.org:5601/s/servicex/app/dashboards?"\
                "auth_provider_hint=anonymous1#/view/2d2b3b40-f34e-11ed-a6d8-9f6a16cd6d78?"\
                "embed=true&_g=(time:(from:now%2Fd,to:now%2Fd))"\
                "&_a=(filters:!((query:(match_phrase:"\
                "(requestId:'d2ede739-9779-4075-95b1-0c7fae1de408'))),"\
                "(query:(match_phrase:(level:'error')))))&show-time-filter=true"\
                "&hide-filter-bar=true"
    assert create_kibana_link_parameters(initial_log_url, transform_id,
                                         log_level, time_frame) == final_url

    transform_id = "93713b34-2f0b-4d53-8412-8afa98626516"
    log_level = LogLevel.info
    time_frame = TimeFrame.month
    final_url = "https://atlas-kibana.mwt2.org:5601/s/servicex/app/dashboards?"\
                "auth_provider_hint=anonymous1#/view/2d2b3b40-f34e-11ed-a6d8-9f6a16cd6d78?"\
                "embed=true&_g=(time:(from:now-30d%2Fd,to:now))"\
                "&_a=(filters:!((query:(match_phrase:"\
                "(requestId:'93713b34-2f0b-4d53-8412-8afa98626516'))),"\
                "(query:(match_phrase:(level:'info')))))&show-time-filter=true"\
                "&hide-filter-bar=true"
    assert create_kibana_link_parameters(initial_log_url, transform_id,
                                         log_level, time_frame) == final_url

    transform_id = "93713b34-2f0b-4d53-8412-8afa98626516"
    log_level = None
    time_frame = TimeFrame.month
    final_url = "https://atlas-kibana.mwt2.org:5601/s/servicex/app/dashboards?"\
                "auth_provider_hint=anonymous1#/view/2d2b3b40-f34e-11ed-a6d8-9f6a16cd6d78?"\
                "embed=true&_g=(time:(from:now-30d%2Fd,to:now))"\
                "&_a=(filters:!((query:(match_phrase:"\
                "(requestId:'93713b34-2f0b-4d53-8412-8afa98626516')))))"\
                "&show-time-filter=true&hide-filter-bar=true"
    assert create_kibana_link_parameters(initial_log_url, transform_id,
                                         log_level, time_frame) == final_url
