
  create or replace   view SSI_SAP_TO_SNOW.REPORT.divisionsmd
  
   as (
    -- Copyright 2022 Google LLC
-- Copyright 2023 DataSentics
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

with divisions AS (
    SELECT
        TSPA.mandt AS Client_MANDT,
        TSPA.spart AS Division_SPART,
        TSPAT.spras AS LanguageKey_SPRAS,
        TSPAT.vtext AS DivisionName_VTEXT
    FROM
        SSI_SAP_TO_SNOW.RAW.tspa AS TSPA
    LEFT JOIN
        SSI_SAP_TO_SNOW.RAW.tspat AS TSPAT
    ON
        TSPA.MANDT = TSPAT.MANDT
        AND TSPA.SPART = TSPAT.SPART
)

SELECT * FROM divisions
  );

