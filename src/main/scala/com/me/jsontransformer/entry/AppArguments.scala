package com.me.jsontransformer.entry

/**
 * Reading the input arguments as arguments in command line
 * the order of usage is input_file_path, output_file_path, schema_file_path, validate/no
 *
 * @param argumentsToApp : Receive arguments as tuple
 */
case class AppArguments(argumentsToApp: (String, String, String, String)) {

  val inputFile: String = argumentsToApp._1
  val targetFolder: String = argumentsToApp._2
  val schemaFile: String = argumentsToApp._3
  val validator: String = argumentsToApp._4

  (inputFile, targetFolder, schemaFile, validator)
}
