#!/bin/bash

# Change to the Maven repository
cd ~/.m2/repository/org/apache/samoa

for filename in $(find . -not -name '*.sha512' -and -not -name '*.asc' -and -not -name '*_remote.repositories'); do
	if [ ! -f $filename.sha512 ] && [ ! -d $filename ]; then
		echo $filename
		gpg --print-md SHA512 $filename > $filename.sha512
	fi
done
