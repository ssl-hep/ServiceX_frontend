# ServiceX_frontend
 Client access library for ServiceX

# Introduction

Client code for accessing ServiceX

Interface Specification:

- Feed it a `qastle` AST, a dataset specification(s), and an output format
- It will return the data to you or an excemption containing something suitable about why it failed

Some goals

- This is not the code that most people will use in the end, so the interface will be a little technical
- Support up to 100 simultanious queries without knocking over your laptop
  - 100 counts as seperate calls into the library and number of datasets
- Should run from inside Jupyter as well as from the command line
- Should support simple running with minimal fuss
- Initial return formats: pandas and awkward
- Python 3.7 or above
- The user will only request data that can fit in memory (no work to do chucked data or anything similar).