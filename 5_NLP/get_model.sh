#!/bin/bash
# Code Originated from: https://gist.github.com/tanaikech/f0f2d122e05bf5f971611258c22c110f
fileid="1l_ZZMfZpe1YdLMDkgaLJ9ZTEhQ8086oP"
filename="Model_Template.zip"
curl -c ./cookie -s -L "https://drive.google.com/uc?export=download&id=${fileid}" > /dev/null
curl -Lb ./cookie "https://drive.google.com/uc?export=download&confirm=`awk '/download/ {print $NF}' ./cookie`&id=${fileid}" -o ${filename}