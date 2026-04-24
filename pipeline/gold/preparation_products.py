# Databricks notebook source
# Databricks notebook source
# Gold layer — raid preparation products
#
# This notebook intentionally defines no DLT tables in Free Edition. The old
# preparation rollups were useful during exploration, but each DLT output also
# creates a hidden materialization table and counts toward the 100-object schema
# quota. The frontend now derives consumable rates from `gold_boss_kill_roster`
# and exports cast-based utility metrics from `silver_player_cast_events`.
