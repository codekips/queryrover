import configparser
import codecs

class AppConfiguration (object) :
    def __init__ (self, props: str| dict[str,str]={}):
        self.config = configparser.ConfigParser()
        if (isinstance(props, dict)):
            self._read_dict(props)
        else:
            with codecs.open(props, 'r', encoding='utf-8') as config_file:
                rover_props = '[rover_config]\n' + config_file.read()
            self.config.read_string(rover_props)
    
    def _read_dict(self, d: dict[str,str]):
        self.config.add_section('rover_config')
        for key, value in d.items():
            self.config.set('rover_config', key, str(value))

    def get_property(self, property_name:str, default_value:str=""):
        """ Returns a property"""
        if self.config.has_option('rover_config', property_name):
            return self.config.get('rover_config', property_name)
        return default_value