"""Process datastore TOML files into the form needed by the web API.

1. Read all dataset metadata .TOML files
2. Validate all datasets against the schema
3. Create files for use by the web API server

"""
# /// script
# dependencies = ['orjson', 'rtoml', 'jsonschema_rs', 'rich', 'echosms', 'numpy']
# ///
# %%
from pathlib import Path
import orjson
import rtoml
import jsonschema_rs
from rich import print as rprint
import numpy as np
import requests
import uuid
import shutil
import os
from datetime import datetime, timezone
from echosms import plot_specimen
from shutil import make_archive

datasets_source_dir = Path.home()/'datasets'
datastore_final_dir = Path.home()/'datastore'

# empty out datastore_final_dir
if os.path.exists(datastore_final_dir):
    shutil.rmtree(datastore_final_dir)
datastore_final_dir.mkdir(exist_ok=True)

schema_url = 'https://raw.githubusercontent.com/ices-tools-dev/echoSMs/refs/heads/main/data_store/schema/v1/anatomical_data_store.json'
# Download the anatomical datastore schema from github
schema = requests.get(schema_url).json()
validator = jsonschema_rs.validator_for(schema)

metadata_file = 'metadata.toml'
metadata_final_filename = 'metadata_all_autogen.json'

def large_shape(row):
    """Identify large shape datasets."""
    if row['shape_type'] == 'voxels' and np.array(row['shapes'][0]['mass_density']).size > 1e3:
        return True

    if row['shape_type'] == 'categorised voxels' and\
       np.array(row['shapes'][0]['categories']).size > 1e3:
        return True

    if row['shape_type'] == 'surface' and len(row['shapes'][0]['x']) > 500:
        return True

# Read in all .toml files that we can find, add/update the dataset_id and dataset_size
# attributes, flatten, and generate an image of each specimen. For specimens with large
# shape data, save that to a seprate json file. Then write out a json file with all specimen
# data in it (except for the large shape data).

dataset = []
error_count = 0
rprint(f'Using datasets in [green]{datasets_source_dir}')
rprint(f'Writing outputs to [green]{datastore_final_dir}\n')
for path in datasets_source_dir.iterdir():
    if path.is_dir():

        # There may be a metadata.toml file and one or more specimen*.toml files.
        meta_file = path/metadata_file
        if meta_file.exists():
            metadata = rtoml.load(meta_file)
        else:
            metadata = {}

        rprint('Reading dataset [orange1]' + path.name)

        # load each .toml file and combine into one echoSMs datastore structure
        # this can take lots of memory, but we do this on a capable machine...
        for ff in path.glob('specimen*.toml'):
            print('  Loading ' + ff.name, end='')

            data = rtoml.load(ff)  # load the specimen data
            data.update(metadata)  # add in metadata if present

            # Update things the datastore is responsible for
            if data['uuid'] == '':
                data['uuid'] = str(uuid.uuid4())
            data['version_time'] = datetime.now(timezone.utc).isoformat()
            data['dataset_size'] = sum(file.stat().st_size for file in Path(path).rglob('*'))/2**20
            data['dataset_size_units'] = 'megabyte'

            # Validate the specimen data
            errored = False
            for error in validator.iter_errors(data):
                print(error.instance_path)
                print(error.evaluation_path)
                print(error.schema_path)
                # rprint(f'\n[yellow] Validation error with {error.message}', end='')
                # rprint('[orange4]' + error.message)
                errored = True

            if errored:
                rprint('\n[red]Validation failed ✗')
                error_count += 1
            else:
                rprint(' [green]Validation passed ✓', end='')

                # Write out to a staging directory
                rprint(' Writing specimen', end='')

                # Make a shape image for later use
                image_file = str(datastore_final_dir/data['uuid']) + '.png'
                plot_specimen(data, title=data['specimen_name'], savefile=image_file, dpi=200)

                if large_shape(data):
                    rprint(' (large shape)', end='')
                    large_shape_file = data['uuid'] + '.json'

                    # write out the shape information
                    json_bytes = orjson.dumps(data['shapes'])
                    with open(datastore_final_dir/large_shape_file, 'wb') as f:
                        f.write(json_bytes)
                    data['large_shape_ref'] = large_shape_file

                    # replace the shape info with just the metadata
                    s_metadata = []
                    for s in data['shapes']:
                        ss = {k: v for k, v in s.items()
                                if k in ['anatomical_feature', 'name', 'boundary']}
                        s_metadata.append(ss)

                    data['shapes'] = s_metadata

                print('')

                dataset.append(data)

if error_count:
    rprint(f'[red]{error_count} datasets failed the verification')

print('\nWriting a combined metadata file')
json_bytes = orjson.dumps(dataset)
with open(datastore_final_dir/metadata_final_filename, 'wb') as f:
    f.write(json_bytes)

rprint(f'Compressing all data into [green]{datastore_final_dir.with_suffix(".zip")}')
make_archive(str(datastore_final_dir), 'zip', datastore_final_dir)
