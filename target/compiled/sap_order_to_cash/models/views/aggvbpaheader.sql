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

with aggvbpaheader AS (
    SELECT VBPA.mandt, VBPA.vbeln, VBPA.posnr,
    
        MAX(CASE WHEN VBPA.PARVW = 'AG' THEN VBPA.KUNNR ELSE NULL END) AS SoldToPartyHeader_KUNNR,
        MAX(CASE WHEN VBPA.PARVW = 'AG' THEN KNA1.name1 ELSE NULL END) AS SoldToPartyHeaderName_KUNNR,
        MAX(CASE WHEN VBPA.PARVW = 'WE' THEN VBPA.KUNNR ELSE NULL END) AS ShipToPartyHeader_KUNNR,
        MAX(CASE WHEN VBPA.PARVW = 'WE' THEN KNA1.name1 ELSE NULL END) AS ShipToPartyHeaderName_KUNNR,
        MAX(CASE WHEN VBPA.PARVW = 'RE' THEN VBPA.KUNNR ELSE NULL END) AS BillToPartyHeader_KUNNR,
        MAX(CASE WHEN VBPA.PARVW = 'RE' THEN KNA1.name1 ELSE NULL END) AS BillToPartyHeaderName_KUNNR,
        MAX(CASE WHEN VBPA.PARVW = 'RG' THEN VBPA.KUNNR ELSE NULL END) AS PayerHeader_KUNNR,
        MAX(CASE WHEN VBPA.PARVW = 'RG' THEN KNA1.name1 ELSE NULL END) AS PayerHeaderName_KUNNR

        -- MAX(IF((VBPA.PARVW = 'AG'), VBPA.KUNNR, NULL)) AS SoldToPartyHeader_KUNNR,
        -- MAX(IF((VBPA.PARVW = 'AG'), KNA1.name1, NULL)) AS SoldToPartyHeaderName_KUNNR,
        -- MAX(IF((VBPA.PARVW = 'WE'), VBPA.KUNNR, NULL)) AS ShipToPartyHeader_KUNNR,
        -- MAX(IF((VBPA.PARVW = 'WE'), KNA1.name1, NULL)) AS ShipToPartyHeaderName_KUNNR,
        -- MAX(IF((VBPA.PARVW = 'RE'), VBPA.KUNNR, NULL)) AS BillToPartyHeader_KUNNR,
        -- MAX(IF((VBPA.PARVW = 'RE'), KNA1.name1, NULL)) AS BillToPartyHeaderName_KUNNR,
        -- MAX(IF((VBPA.PARVW = 'RG'), VBPA.KUNNR, NULL)) AS PayerHeader_KUNNR,
        -- MAX(IF((VBPA.PARVW = 'RG'), KNA1.name1, NULL)) AS PayerHeaderName_KUNNR
    FROM
        SSI_SAP_TO_SNOW.RAW.vbpa AS VBPA
    INNER JOIN SSI_SAP_TO_SNOW.RAW.kna1 AS KNA1
        ON
        VBPA.mandt = KNA1.mandt
        AND VBPA.kunnr = KNA1.kunnr
    GROUP BY VBPA.mandt, VBPA.vbeln, VBPA.posnr
)

SELECT * FROM aggvbpaheader