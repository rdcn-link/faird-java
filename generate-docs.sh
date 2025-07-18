#!/bin/bash

SCALA_SRC="src/main/scala"
JAVA_SRC="src/main/java"
SCALADOC_OUT="doc/scaladoc"
JAVADOC_OUT="doc/javadoc"

mkdir -p "$SCALADOC_OUT" "$JAVADOC_OUT"

# ==== Scaladoc ====
echo "Generating Scaladoc..."
find "$SCALA_SRC" -name "*.scala" > scalafiles.txt

if command -v scaladoc >/dev/null 2>&1; then
  if [ -s scalafiles.txt ]; then
    scaladoc @scalafiles.txt -d "$SCALADOC_OUT"
  else
    echo "No Scala files found. Skipping Scaladoc."
  fi
else
  echo "scaladoc not found. Please install Scala or use sbt doc instead."
fi
rm -f scalafiles.txt

# ==== Javadoc ====
echo "Generating Javadoc..."
find "$JAVA_SRC" -name "*.java" > javafiles.txt

if command -v javadoc >/dev/null 2>&1; then
  if [ -s javafiles.txt ]; then
    javadoc @javafiles.txt -d "$JAVADOC_OUT"
  else
    echo "No Java files found. Skipping Javadoc."
  fi
else
  echo "javadoc not found. Please install JDK."
fi
rm -f javafiles.txt
