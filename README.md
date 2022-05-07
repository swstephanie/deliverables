# Address Matching


This project discusses the address matching between two datasets. We try to match the two tables using usaddress package in Python and Placekey (an external API).

## How to use this algorithm
we have put all functions in the helper_function.py. You can check them to know how to standardize the address using usaddress package, how to generate Placekey, and how to use owner names to match the addresses. 

If you just want to get a result from the two tables, find the integrated function in the main.py and provide the sql statement for the two datasets. **It is important to include the information about the address(full address, state, city, zipcode), the ownername and so on.**

### more about the py files 
- main.py:   
an integrated function of our algorithm. You can call it to get a csv file of the matched addresses 
- helper_funcitons.py:     
check it to have a better idea of how to standardize the address, how to get a placekey and how to match the addresses by owner names
- name_node_dict.py:    
change it if you want to replace the common words in the owner names with different words
- credlib.py:   
You need to provide your account, your password, your username and the api key of Placekey here. 



## Check the library and packages we use 
### [usaddress](https://usaddress.readthedocs.io/en/latest/) 
usaddress is a python library for parsing unstructured address strings into address components, using advanced NLP methods.
check the sample output below
```sh
>>> import usaddress
>>> usaddress.parse('Robie House, 5757 South Woodlawn Avenue, Chicago, IL 60637')

[('Robie', 'BuildingName'),
('House,', 'BuildingName'),
('5757', 'AddressNumber'),
('South', 'StreetNamePreDirectional'),
('Woodlawn', 'StreetName'),
('Avenue,', 'StreetNamePostType'),
('Chicago,', 'PlaceName'),
('IL', 'StateName'),
('60637', 'ZipCode')]
```
### [Placekey](https://www.placekey.io/) 
Placekey is a free, universal standard identifier for any physical place, so that the data pertaining to those places can be shared across organizations easily.
In this project, we use python package. Documentation can be found [here](https://pypi.org/project/placekey/). 


```sh
>>> place = {
>>>   "street_address": "598 Portola Dr",
>>>   "city": "San Francisco",
>>>   "region": "CA",
>>>   "postal_code": "94131",
>>>   "iso_country_code": "US"
>>> }
>>> pk_api.lookup_placekey(**place, strict_address_match=True)
{'query_id': '0', 'placekey': '227@5vg-82n-pgk'}
```


##### End of this document 
