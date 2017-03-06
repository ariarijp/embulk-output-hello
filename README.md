# Hello output plugin for Embulk

An example of Java Output Plugin for Embulk.

## Overview

* **Plugin type**: output
* **Load all or nothing**: no
* **Resume supported**: no
* **Cleanup supported**: yes

## Configuration

- **option1**: description (integer, required)
- **option2**: description (string, default: `"myvalue"`)
- **option3**: description (string, default: `null`)

## Example

```yaml
out:
  type: hello
  option1: 1234
  option2: foo
  option3: var
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
