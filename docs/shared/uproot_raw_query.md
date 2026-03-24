```python
query = [
         {
          'treename': 'reco',
          'filter_name': ['/mu.*/', 'runNumber', 'lbn', 'jet_pt_*'],
          'cut':'(count_nonzero(jet_pt_NOSYS>40e3)>=4)'
         },
         {
          'copy_histograms': ['CutBookkeeper*', '/cflow.*/', 'metadata', 'listOfSystematics']
         }
        ]
```
