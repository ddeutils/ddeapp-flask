from typing import Dict
from flask_assets import Bundle

# If you want to re-create bundle manually: `bundles[<bundle-name>].build()`
bundles: Dict[str, Bundle] = {

    # Main asset bundles
    'home_js': Bundle(
        'js/utilities.js',
        filters='jsmin',
        output='assets/gen/home.%(version)s.min.js'
    ),
    'home_css': Bundle(
        Bundle(
            'css/base.css',
            'css/header.css',
            'css/footer.css',
            'css/search.css',
            'css/paginate.css',
            'css/alert.css',
            merge=True,
        ),
        Bundle(
            'scss/main.scss',
            'scss/content-alert.scss',
            filters='pyscss',
            depends=('scss/*.scss', ),
        ),
        filters='cssmin',
        output='assets/gen/home.%(version)s.min.css',
        extra={'rel': 'stylesheet/css'},
    ),
}
