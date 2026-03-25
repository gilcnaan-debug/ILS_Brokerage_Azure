# Databricks notebook source
# MAGIC %sql
# MAGIC UPDATE metadata.mastermetadata
# MAGIC SET PipelineRunStatus = 'Failed'
# MAGIC WHERE TableID = 'SL11'