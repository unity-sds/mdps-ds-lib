from mdps_ds_lib.lib.utils.factory_abstract import FactoryAbstract
from mdps_ds_lib.stac_fast_api_client.sfa_client_base import SFAClientBase


class SFAClientFactory(FactoryAbstract):
    NO_AUTH = 'NO_AUTH'
    BASIC_AUTH = 'BASIC_AUTH'
    COOKIE_AUTH = 'COOKIE_AUTH'
    BEARER_AUTH = 'BEARER_AUTH'

    def get_instance(self, class_type, **kwargs) -> SFAClientBase:
        if class_type == self.NO_AUTH:
            from mdps_ds_lib.stac_fast_api_client.sfa_client_no_auth import SFAClientNoAuth
            return SFAClientNoAuth(kwargs['ds_url'], kwargs['ds_stage'])
        if class_type == self.BASIC_AUTH:
            from mdps_ds_lib.stac_fast_api_client.sfa_client_basic_auth import SFAClientBasicAuth
            return SFAClientBasicAuth(kwargs['username'], kwargs['password'], kwargs['ds_url'], kwargs['ds_stage'])
        if class_type == self.COOKIE_AUTH:
            from mdps_ds_lib.stac_fast_api_client.sfa_client_cookie_token import SFAClientCookieToken
            return SFAClientCookieToken(kwargs['auth_key'], kwargs['auth_value'], kwargs['ds_url'], kwargs['ds_stage'])
        if class_type == self.BEARER_AUTH:
            from mdps_ds_lib.stac_fast_api_client.sfa_client_bearer import SFAClientBearer
            return SFAClientBearer(kwargs['bearer_token'], kwargs['ds_url'], kwargs['ds_stage'])
        raise NotImplementedError(f'unknown class type: {class_type}')
