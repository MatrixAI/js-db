{
  'targets': [{
    'target_name': 'leveldb',
    'include_dirs': [
      "<!(node -e \"require('napi-macros')\")"
    ],
    'dependencies': [
      '<(module_root_dir)/deps/leveldb/leveldb.gyp:leveldb'
    ],
    'sources': ['./src/leveldb/index.cpp'],
    'conditions': [
      ['OS=="linux"', {
        'cflags': [ '-std=c99', '-Wpedantic' ],
        'cflags_cc': [ '-std=c++17', '-Wpedantic' ],
      }],
      ['OS=="win"', {
        'defines': [
          # See: https://github.com/nodejs/node-addon-api/issues/85#issuecomment-911450807
          '_HAS_EXCEPTIONS=0'
        ],
        'msvs_settings': {
          'VCCLCompilerTool': {
            'RuntimeTypeInfo': 'false',
            'EnableFunctionLevelLinking': 'true',
            'ExceptionHandling': '2',
            'DisableSpecificWarnings': [
              '4355',
              '4530',
              '4267',
              '4244',
              '4506',
            ],
            'AdditionalOptions': [ '/std:c++17' ]
          }
        },
      }],
      ['OS=="mac"', {
        # OSX symbols are exported by default
        # if 2 different copies of the same symbol appear in a process
        # it can cause a conflict
        # this prevents exporting the symbols
        # the `+` prepends these flags
        'cflags+': [ '-fvisibility=hidden' ],
        'cflags_cc+': [ '-fvisibility=hidden' ],
        'xcode_settings': {
          # Minimum mac osx target version (matches node v16.14.2)
          'MACOSX_DEPLOYMENT_TARGET': '10.13',
          # This is also needed to prevent exporting of symbols
          'GCC_SYMBOLS_PRIVATE_EXTERN': 'YES',
          'OTHER_CFLAGS': [
            '-std=c99',
            '-arch x86_64',
            '-arch arm64'
          ],
          'OTHER_CPLUSPLUSFLAGS': [
            '-std=c++17'
            '-arch x86_64',
            '-arch arm64'
          ],
          'OTHER_LDFLAGS': [
            '-arch x86_64',
            '-arch arm64'
          ]
        }
      }],
      ['OS == "android"', {
        'cflags': [ '-std=c99', '-fPIC' ],
        'cflags_cc': [ '-std=c++17', '-fPIC '],
        'ldflags': [ '-fPIC' ],
        'cflags!': [
          '-fPIE',
          '-mfloat-abi=hard',
        ],
        'cflags_cc!': [
          '-fPIE',
          '-mfloat-abi=hard',
        ],
        'ldflags!': [ '-fPIE' ],
      }],
      ['target_arch == "arm"', {
        'cflags': [ '-mfloat-abi=hard' ],
        'cflags_cc': [ '-mfloat-abi=hard '],
      }],
    ]
  }]
}
