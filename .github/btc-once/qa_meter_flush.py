"""QA-only passive snapshots before the worker's existing process teardown.

The counters are cumulative: reports must use the latest snapshot, not sum it.
No browser/reader/session behavior or cleanup timeout is changed.
"""
def install():
    from capacity_meter import BrowserMeter
    original = BrowserMeter.count
    def count(meter, key, n=1):
        original(meter, key, n)
        if key == 'browser_read_ms':
            meter.receive(dict(phase='map_complete', cumulative=True,
                               host=dict(meter.host), peak_tabs=meter.peak_tabs,
                               maps={job:dict(values) for job,values in meter.maps.items()}))
    BrowserMeter.count = count
