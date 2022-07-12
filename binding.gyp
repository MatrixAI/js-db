{
  'targets': [{
    'target_name': 'native',
    'include_dirs': [
      "<!(node -e \"require('napi-macros')\")"
    ],
    'dependencies': [
      '<(module_root_dir)/deps/rocksdb/rocksdb.gyp:rocksdb'
    ],
    'sources': [
      './src/native/napi/batch.cpp',
      './src/native/napi/database.cpp',
      './src/native/napi/debug.cpp',
      './src/native/napi/index.cpp',
      './src/native/napi/iterator.cpp',
      './src/native/napi/snapshot.cpp',
      './src/native/napi/transaction.cpp',
      './src/native/napi/utils.cpp',
      './src/native/napi/worker.cpp',
      './src/native/napi/workers/batch_workers.cpp',
      './src/native/napi/workers/database_workers.cpp',
      './src/native/napi/workers/iterator_workers.cpp',
      './src/native/napi/workers/transaction_workers.cpp',
      './src/native/napi/workers/snapshot_workers.cpp'
    ],
    'conditions': [
      ['OS=="linux"', {
        'cflags': [ '-std=c99', '-Wpedantic' ],
        'cflags!': [ '-fno-tree-vrp', '-fno-exceptions' ],
        'cflags_cc': [ '-std=c++17', '-Wpedantic' ],
        'cflags_cc!': [ '-fno-exceptions' ],
      }],
      ['OS=="win"', {
        'defines': [
          # See: https://github.com/nodejs/node-addon-api/issues/85#issuecomment-911450807
          '_HAS_EXCEPTIONS=0',
          'OS_WIN=1',
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
          },
          'VCLinkerTool': {
            'AdditionalDependencies': [
              # SDK import libs
              'Shlwapi.lib',
              'rpcrt4.lib'
            ]
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
      ['target_arch == "arm"', {
        'cflags': [ '-mfloat-abi=hard' ],
        'cflags_cc': [ '-mfloat-abi=hard '],
      }],
    ]
  }]
}
