from os import system,popen
from collections import defaultdict
import numpy

command="gsutil cp gs://ap-parcel-data-dev/PARCEL_DATA_17032018.csv gs://ap-parcel-data-test/PARCEL_DATA_17032018.csv"
popen(command).read()

command="gsutil cp gs://ap-parcel-data-dev/PARCEL_DATA_18032018.csv gs://ap-parcel-data-test/PARCEL_DATA_18032018.csv"
popen(command).read()

command="gsutil cp gs://ap-parcel-data-dev/PARCEL_DATA_19032018.csv gs://ap-parcel-data-test/PARCEL_DATA_19032018.csv"
popen(command).read()

print("files copied")

# gcloud auth login
# ls -lrt
# cwd
# cd
