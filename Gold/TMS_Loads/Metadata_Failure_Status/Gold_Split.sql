-- Databricks notebook source
UPDATE metadata.mastermetadata
SET PipelineRunStatus = 'Failed'
WHERE TableID = 'GL2'