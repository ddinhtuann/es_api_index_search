import pyarrow as pa
import os
os.environ['ARROW_LIBHDFS_DIR'] = '/opt/cloudera/parcels/CDH-5.14.4-1.cdh5.14.4.p0.3/lib64/'
hdfs_itf = pa.hdfs.connect(host = '10.0.68.37', port = 9002, user = 'coretech')
dest = '/user/coretech/data/'
hdfs_itf.upload(dest, file_name)
