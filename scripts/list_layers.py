import requests

def _post(service, _params={'f': 'pjson'}, ret_json=True):
    """Post Request to REST Endpoint

    Required:
    service -- full path to REST endpoint of service

    Optional:
    _params -- parameters for posting a request
    ret_json -- return the response as JSON.  Default is True.
    """
    proxies={
        'http':'http://madara.inei.gob.pe:3128',
        #'http': 'http://kira.inei.gob.pe:3128',
     #   'https': 'https://kira.inei.gob.pe:3128',

    }

    auth=requests.auth.HTTPProxyAuth('fsoto', 'MBs0p0rt303')
    r = requests.get(service, params=_params, proxies=proxies)

    # make sure return
    if r.status_code != 200:
        raise NameError('"{0}" service not found!\n{1}'.format(service, r.raise_for_status()))
    else:
        if ret_json:
            return r.json()
        else:
            return r

def list_services(service='http://arcgis.inei.gob.pe:6080/arcgis/rest/services'):
    """returns a list of all services

    Optional:
    service -- full path to a rest service
    """
    all_services = []
    r = _post(service)
    for s in r['services']:
        all_services.append('/'.join([service, s['name'], s['type']]))
    for s in r['folders']:
        new = '/'.join([service, s])
        endpt = _post(new)
        for serv in endpt['services']:
           all_services.append('/'.join([service, serv['name'], serv['type']]))
    return all_services

def list_layers(service):
    """lists all layers in a mapservice

    Returns a list of field objects with the following properties:
        name -- name of layer
        id -- layer id (int)
        minScale -- minimum scale range at which layer draws
        maxScale -- maximum scale range at which layer draws
        defaultVisiblity -- the layer is visible (bool)
        parentLayerId -- layer id of parent layer if in group layer (int)
        subLayerIds -- list of id's of all child layers if group layer (list of int's)

    Required:
    service -- full path to mapservice
    """
    r = _post(service)
    if 'layers' in r:
        return [layer(p) for p in r['layers']]
    return

def list_tables(service):
    """List all tables in a MapService"""
    r = _post(service)
    if 'tables' in r:
        return [table(p) for p in r['tables']]
    return None

class layer:
    """class to handle layer info"""
    def __init__(self, lyr_dict):
        for key, value in lyr_dict.items():
            setattr(self, key, value)

class table:
    """class to handle table info"""
    def __init__(self, tab_dict):
        for key, value in tab_dict.items():
            setattr(self, key, value)

if __name__ == '__main__':

    for serv in list_services():
        print (serv)

        print ('Layers:\n')
        for lyr in [l.name for l in list_layers(serv)]:
            print (lyr)
        print ('\n\n')