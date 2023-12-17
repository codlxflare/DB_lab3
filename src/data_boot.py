import pandas

def boot_file():
    fp = pandas.read_csv('file/nyc_yelow_tiny.csv')
    if 'Airport_fee' in fp.columns:
        fp = fp.drop(columns=['Airport_fee'])
    return fp