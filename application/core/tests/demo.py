from application.core.legacy.base import TblCatalog, get_config
from application.core.models import Catalog
from application.core.validators import Table, Profile, Function, Pipeline
from application.core.statements import ProfileStatement
from pprint import pprint, pformat


def test_table_validator():
    aam = get_config(
            config_name='imp_min_max_service_level',
            config_prefix='imp',
            folder_config='catalog',
            config_prefix_file='catalog'
        )
    # pprint(
    #     aam, indent=2, sort_dicts=False, width=200
    # )

    profile = Profile.parse_obj({
        'features': [
            {'name': name, 'datatype': data}
            for name, data in aam['create']['features'].items()
        ],
        'primary_key': aam['create']['primary_key'],
        'foreign_key': {
            'class_a': 'author( class_a )'
        }
    })
    pprint(profile.dict(), indent=2, sort_dicts=False, width=200)
    print('-' * 150)
    data = {
        'name': aam['config_name'],
        'type': 'py',
        'create': {
            'features': aam['create']['features'],
            'primary_key': aam['create']['primary_key'],
            'partition': {
                'type': 'range',
                'columns': ['update_date']
            },
            'foreign_key': {
                'class_a': 'author( class_a )'
            },
        },
        'function': aam['function'],
        'initial': aam['initial'],
        'version': '2022-01-01'
    }
    model = Table.parse_obj(data)
    pprint(model.dict(by_alias=False), indent=2, sort_dicts=False, width=200)
    print('=' * 150)


def test_table_validator_2():
    aam = get_config(
            config_name='ai_report_article_listing_master',
            config_prefix='ai',
            folder_config='catalog',
            config_prefix_file='catalog'
        )
    print('-' * 150)
    data = {
        'name': aam['config_name'],
        'type': 'sql',
        'create': {
            'features': aam['create']['features'],
            'primary_key': aam['create']['primary_key'],
        },
        'update': aam['update'],
        'initial': {},
        'version': '2022-01-01'
    }
    model = Table.parse_obj(data)
    pprint(model.dict(by_alias=False), indent=2, sort_dicts=False, width=200)
    print('=' * 150)


def test_table_default():
    aam = get_config(
        config_name='imp_min_max_service_level',
        config_prefix='imp',
        folder_config='catalog',
        config_prefix_file='catalog'
    )
    data = {
        'name': aam['config_name'],
        'type': 'py',
        'profile': {'features': []},
        'function': aam['function'],
        'initial': aam['initial']
    }
    model = Table.parse_obj(data)
    pprint(model.dict(by_alias=False), indent=2, sort_dicts=False, width=200)


def test_function_validator():
    cie = get_config(
        config_name='func_count_if_exists',
        config_prefix='',
        folder_config='function',
        config_prefix_file='func'
    )
    pprint(
        cie, indent=2, sort_dicts=False, width=200
    )
    data: dict = {
        'name': cie['config_name'],
        'version': cie['version'],
        'create': cie['create'],
    }
    model = Function.parse_obj(data)
    pprint(model.dict(by_alias=False), indent=2, sort_dicts=False, width=200)


def test_pipeline_validator():
    pam: dict = get_config(
        config_name='after_sync_article_master',
        config_prefix='',
        folder_config='pipeline',
        config_prefix_file='pipeline'
    )
    # pprint(
    #     pam, indent=2, sort_dicts=False, width=200
    # )
    data = {
        'name': pam['config_name'],
        'id': pam['id'],
        'priority': pam['priority'],
        'trigger': pam['trigger'],
        'alert': pam['alert'],
        'schedule': [],
        'nodes': pam['nodes']
    }
    model = Pipeline.parse_obj(data)
    pprint(model.dict(by_alias=False), indent=2, sort_dicts=False, width=200)


if __name__ == '__main__':
    # test_table_validator()
    # test_table_validator_2()
    # test_table_default()
    # test_function_validator()
    test_pipeline_validator()
