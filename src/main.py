"""Proof of concept of echoSMs anatomical data store RESTful API using FastAPI."""

from fastapi import FastAPI, Query, HTTPException, Path as fPath
from fastapi.responses import Response, FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from typing import Annotated
from pathlib import Path
import orjson
import jmespath
import copy
from datetime import datetime as dt
from stat import S_IFDIR, S_IFREG
from stream_zip import ZIP_64, stream_zip
from urllib.request import urlretrieve
from zipfile import ZipFile
import os
import shutil


schema_url = 'https://ices-tools-dev.github.io/echoSMs/schema/data_store_schema/'

metadata_filename = 'metadata_all_autogen.json'

datasets_dir = Path.home()/'datastore'
favicon_path = 'echoSMs_logo_auto_colour.svg'

"Obtain the datastore and load into memory."
print('Loading datastore from local files')
with open(datasets_dir/metadata_filename, 'rb') as f:
    json_bytes = f.read()
    all_datasets = orjson.loads(json_bytes)

####################################################################################################
app = FastAPI(title='The echoSMs web API',
              openapi_tags=[{'name': 'v2',
                             'description': ''},])


# /v2/specimens endpoint query parameter definitions via a Pydantic model

# The 'model_style' parameter is used to indicate how to form the jmespath query for that
# attribute. Valid values are:
# - not present - the query will look for a simple attribute=value in the metadata
# - ('array',) - the query will treat the attribute as an array and try to match against any string
#                item in the array
# - ('nested', 'group_name') - the query will look for the attribute under 'group_name' and 
#                              do a simple attribute=value string search in the metadata
# 
# Currently, the nested only does a string search and doesn't support arrays or numbers in nested
# attributes.

class SpecimenQuery_v2(BaseModel):  # noqa
    species: str | None = Field(None, title='Species', description="The scientific species name")
    uuid: str | None = Field(None, title='Specimen UUID', description="The specimen UUID")
    specimen_name: str | None = Field(None, title='Specimen name', description="The specimen name")
    dataset_uuid: str | None = Field(None, title='Dataset UUID', description="The dataset UUID")
    dataset_name: str | None = Field(None, title='Dataset name', description="The dataset name")
    family: str | None = Field(None, title='Family', description="The scientific family name")
    genus: str | None = Field(None, title='Genus', description="The scientific genus name")
    activity_name: str | None = Field(None, title='Activity name', description="The activity name")
    sex: str | None = Field(None, title='Sex of the organism', description='The sex of the organism')
    imaging_method: str | None = Field(None, title='Imaging method', description="The imaging method used")
    specimen_condition: str | None = Field(None, title='Specimen condition',
                                            description="The specimen condition")
    model_type: str | None = Field(None, title='Model type', description="The model type used")
    shape_type: str | None = Field(None, title='Shape type', description="The shape type used")
    shape_method: str | None = Field(None, title='Shape method', description="The shape method")
    vernacular_names: str | None = Field(None, title='Vernacular name',
                                         description="A vernacular name",
                                         query_style=('array',))
    anatomical_category: str | None = Field(None, title='Anatomical category',
                                            description="The anatomical category")
    anatomical_feature: str | None = Field(None, title='Anatomical feature', 
                                description="Specimen contains a shape with this anatomical feature",
                                query_style=('nested', 'shapes'))
    boundary: str | None = Field(None, title='Shape boundary',
                                 description="The shape boundary",
                                 query_style=('nested', 'shapes'))
    version_investigators: str | None = Field(None, title='Investigator name',
                                description="An investigator name",
                                query_style=('array',))
    aphia_id: int | None = Field(None, title='AphiaID',
                               description='The [aphiaID](https://www.marinespecies.org/aphia.php)')


####################################################################################################
@app.get("/v2/specimens",
         summary="Get specimen metadata with optional filtering. Does not return shape data.",
         response_description='A list of specimen metadata',
         tags=['v2'])
async def get_specimens_v2(query: Annotated[SpecimenQuery_v2, Query()]):  # noqa
        # Return all specimens if no query parameters are given
        if not query.model_fields_set:
            return remove_shape_data(all_datasets)

        # Build a jmespath query string from the query parameters
        q = []
        for (attr_name, value) in query:
            if value is None:
                continue

            # the 'query_style' parameter in the query definition ends up as a dict on the
            # json_schema_extra attribute
            if s := query.model_fields[attr_name].json_schema_extra:
                query_style = s['query_style']
            else:
                query_style = (None,)
            
            match query_style[0]:
                case 'array':
                    q.append(f"{attr_name}[?contains(@, '{value}')]")
                case 'nested':
                    q.append(f"{query_style[1]}[?{attr_name} == '{value}']")
                case _:  # A normal top level attribute
                    if isinstance(value, int) or isinstance(value, float):
                        q.append(f"{attr_name} == `{value}`")
                    else:
                        q.append(f"{attr_name} == '{value}'")
            
        specimens = jmespath.search('[?' + ' && '.join(q) + ']', all_datasets)

        return remove_shape_data(specimens)


####################################################################################################
@app.get("/v2/specimen/{uuid}/data",
         summary='Get all specimen data with the given UUID',
         response_description='Specimen data structured as per the echoSMs data '
                              f'store [schema]({schema_url})',
         tags=['v2'])
async def get_specimen_shape_v2(uuid: Annotated[str, fPath(description='The specimen UUID')]):  # noqa

    s = specimen(uuid)
    if not s:
        raise HTTPException(status_code=404, detail=f'Specimen {uuid} not found')

    return s


####################################################################################################
@app.get("/v2/specimen/{uuid}/image",
         summary='Get an image of the specimen shape with the given UUID',
         response_description='An image of the specimen shape',
         tags=['v2'],
         response_class=Response,
         responses={200: {'content': {'image/png': {}}}})
async def get_specimen_image_v2(uuid: Annotated[str, fPath(description='The specimen UUID')]):  # noqa

    image_file = Path(f'{datasets_dir/uuid}.png')
    return FileResponse(image_file)


####################################################################################################
@app.get("/v2/dataset/{dataset_uuid}/all",
         summary='Get all data with the given dataset_uuid, including any raw data',
         response_description='A zipped file containing all data for the dataset',
         tags=['v2'])
async def get_dataset(dataset_uuid: Annotated[str, fPath(description='The dataset UUID')]):  # noqa

    return {"message": "Not yet implemented"}

    # The plan: zip up all files in the directory with the same name as the given
    # dataset_uuid. If such a directory doesn't exist, raise HTTPException

    # zip up the dataset and stream out
    return StreamingResponse(stream_zip(get_dir_items(datasets_dir/dataset_uuid)),
                             media_type='application/zip',
                             headers={'Content-Disposition':
                                      f'attachment; filename={dataset_uuid}.zip'})

####################################################################################################
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():  # noqa
    return FileResponse(favicon_path, media_type="image/svg+xml")

#============================================================================
# Helper functions

def specimen(sid):
    """Find specimen with given uuid, reading the shape from file if needed."""
    s = jmespath.search(f"[?uuid == '{sid}']", all_datasets)

    if not s:
        return None

    s = copy.deepcopy(s[0])

    # If the shape is not in all_datasets (because it is large), load it
    ref_key = 'large_shape_ref'
    if ref_key in s:
        if isinstance(s[ref_key], str):
            with open(datasets_dir/s[ref_key], 'r') as f:
                json_bytes = f.read()  # loads it all into memory
                s['shapes'] = orjson.loads(json_bytes)
            del s[ref_key]

    return s

def remove_shape_data(specimens):
    """Remove all shape data except for some metadata.

    Also remove the large_shape_ref item if present.
    
    Returns a modified copy of the input dict.
    """
    sps_copy = copy.deepcopy(specimens)
    for sp in sps_copy:
        s_metadata = []
        for s in sp['shapes']:
            ss = {k: v for k, v in s.items()
                    if k in ['anatomical_feature', 'name', 'boundary']}
            s_metadata.append(ss)
        sp['shapes'] = s_metadata

        if 'large_shape_ref' in sp:
            del sp['large_shape_ref']

    return sps_copy

def get_dir_items(base_path: Path):
    """Create an iterable of file/directory info for use by stream-zip."""
    for item in base_path.rglob('*'):
        a_name = item.relative_to(base_path).as_posix()  # path within the zip archive
        # need a tuple of (archive_name, modified_time, mode, compression_method, data_source)
        # For directories, data_source must be empty
        if item.is_file():
            with open(item, 'rb') as f:
                yield (a_name, dt.fromtimestamp(item.stat().st_mtime),
                       S_IFREG | 0o644,  # regular file with read/write permissions
                       ZIP_64, (chunk for chunk in iter(lambda: f.read(65536*64), b'')))
        elif item.is_dir():
            yield (a_name + '/',  # trailing slash for directories
                   dt.fromtimestamp(item.stat().st_mtime), S_IFDIR | 0o755, ZIP_64, ())
