import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
import os
from google.cloud import bigquery
import logging
import pandas as pd
import math
import gcsfs
from google.cloud import storage

class CalculoLifeTimeValue(beam.DoFn):
    
    @staticmethod
    def clasificar_ria(ria):
        if ria > 55000:
            return 59
        elif 26270 < ria <= 55000:
            return 59
        elif 12580 <= ria < 26270:
            return 58
        elif 8140 <= ria < 12580:
            return 58
        elif 4070 <= ria < 8140:
            return 57
        elif 0 < ria < 4070:
            return 56
        else:
            return 50

    @staticmethod
    def clasificar_edad(edad):
        if edad >= 0 and edad <= 25:
            return "0-25"
        elif edad >= 26 and edad <= 35:
            return "26-35"
        elif edad >= 36 and edad <= 45:
            return "36-45"
        elif edad >= 46 and edad <= 55:
            return "46-55"
        elif edad >= 56 and edad <= 65:
            return "56-65"
        elif edad >= 66:
            return "66-MAS"
        else:
            return "Fuera de rango"

    def process(self, element):
        tasas_por_industria = {
            "MANUFACTURA": {
                "0-25": 1.0705,
                "26-35": 1.0430,
                "36-45": 1.0143,
                "46-55": 1.0209,
                "56-65": 1.0162,
                "66-MAS": 1
            },
            "SECTOR PUBLICO": {
                "0-25": 1.0636,
                "26-35": 1.0293,
                "36-45": 1,
                "46-55": 1,
                "56-65": 1,
                "66-MAS": 1
            },
            "ADMINISTRACION PUBLICA": {
                "0-25": 1.0636,
                "26-35": 1.0293,
                "36-45": 1,
                "46-55": 1,
                "56-65": 1,
                "66-MAS": 1
            },
            "COMERCIALIZADORAS": {
                "0-25": 1.0707,
                "26-35": 1.0507,
                "36-45": 1.0226,
                "46-55": 1.0168,
                "56-65": 1,
                "66-MAS": 1
            },
            "SERVICIOS": {
                "0-25": 1.1127,
                "26-35": 1.0422,
                "36-45": 1.0180,
                "46-55": 1,
                "56-65": 1,
                "66-MAS": 1
            },
            "INTERMEDIACION FINANCIERA": {
                "0-25": 1.1127,
                "26-35": 1.0422,
                "36-45": 1.0180,
                "46-55": 1,
                "56-65": 1,
                "66-MAS": 1
            },
            "CONSTRUCCION/TRANSPORTE": {
                "0-25": 1.0700,
                "26-35": 1.0450,
                "36-45": 1.0164,
                "46-55": 1.0034,
                "56-65": 1.0070,
                "66-MAS": 1
            },
            "AGROINDUSTRIA Y GANADERIA": {
                "0-25": 1.0551,
                "26-35": 1.0177,
                "36-45": 1.0153,
                "46-55": 1.0113,
                "56-65": 1.0212,
                "66-MAS": 1
            },
            "EXTRACTIVAS": {
                "0-25": 1.1095,
                "26-35": 1.0682,
                "36-45": 1.0270,
                "46-55": 1.0082,
                "56-65": 1.0033,
                "66-MAS": 1
            },
            "ORGANIZACIONES": {
                "0-25": 1.03,
                "26-35": 1.03,
                "36-45": 1.03,
                "46-55": 1.03,
                "56-65": 1.03,
                "66-MAS": 1.03
            },
            "OTROS": {
                "0-25": 1.03,
                "26-35": 1.03,
                "36-45": 1.03,
                "46-55": 1.03,
                "56-65": 1.03,
                "66-MAS": 1.03
            }
        }
        # Sugiero convertirlo en VAR GLOBAL
        r2_fixed = 1.06
        fee_saldo_fixed = 0.0078
        fee_flujo_fixed = 0.0155
        td_fixed = 0.1018
        n_perc75_para_mcdo_2 = 5.75
        RIA_OBJETIVO = 9000
        
        CIC = float(element['CIC'])
        RIA = float(element['RIA'])
        Dens_Cot = float(element['DENS_COT'])
        edad = int(element['EDAD'])
        industria = element['INDUSTRIA']
        n_para_mcdo_2 = 0
        nuevo_nnn = 0

        a = RIA * Dens_Cot * 12 / 10

        nuevo_n = self.clasificar_ria(RIA)

        if edad < nuevo_n:
            nuevo_nnn = nuevo_n - edad

        if edad >= nuevo_n:
            nuevo_nnn = 0

        n = int(round(nuevo_nnn))

        s = 0

        lista_VF_saldo = []
        lista_VF_flujo = []
        lista_VP_saldo = []
        lista_VP_flujo = []

        new_ria = 0
        new_fixed = 0
        aporte_anterior = 0
        aporte_acumulado = 0

        for k in range(1, n + 1):
            rango_edad = self.clasificar_edad(edad + 1)
            r1_fixed = tasas_por_industria[industria][rango_edad]
            edad += 1

            new_ria = RIA * r1_fixed if new_ria == 0 else new_ria * r1_fixed

            new_fixed = 1 * r1_fixed if new_fixed == 0 else new_fixed * r1_fixed

            if aporte_anterior == 0:
                aporte = (new_ria / 10) * 12 * r2_fixed
            else:
                aporte = (new_ria / 10) * 12

            aporte_acumulado1 = aporte_acumulado
            aporte_acumulado += aporte  # Suma acumulativa del aporte

            if aporte_anterior == 0:
                aporte_total = aporte_acumulado
            else:
                aporte_total = aporte_acumulado * r2_fixed
                aporte_acumulado = aporte_total

            aporte_anterior = aporte

            for i in range(1, k + 1):
                s = aporte_total

            fee_saldo_acumulado = (
                (CIC * pow(r2_fixed, k)) + s * Dens_Cot) * fee_saldo_fixed
            valor_presente_saldo = fee_saldo_acumulado / (1 + td_fixed) ** k

            fee_flujo_acumulado = new_ria * Dens_Cot * 12 * fee_flujo_fixed
            valor_presente_flujo = fee_flujo_acumulado / (1 + td_fixed) ** k

            lista_VF_saldo.append(round(fee_saldo_acumulado, 2))
            lista_VF_flujo.append(round(fee_flujo_acumulado, 2))
            lista_VP_saldo.append(round(valor_presente_saldo, 2))
            lista_VP_flujo.append(round(valor_presente_flujo, 2))

            s = 0

        element["n_para_mcdo_2"] = round(n_para_mcdo_2, 4)
        element["n_para_fuga"] = round(n_perc75_para_mcdo_2, 4)
        element["n"] = n
        element["n_ria"] = nuevo_n
        element["new_ria"] = round(new_ria, 4)
        element["vf_comision_saldo"] = round(sum(lista_VF_saldo), 4)
        element["vp_comision_saldo"] = round(sum(lista_VP_saldo), 4)
        element["vf_comision_flujo"] = round(sum(lista_VF_flujo), 4)
        element["vp_comision_flujo"] = round(sum(lista_VP_flujo), 4)

        yield element

class ExtractPeriod(beam.DoFn):
    def process(self, element):
        period = element['periodo']
        yield period

class DeleteFromBigQuery(beam.DoFn):
    def __init__(self, project, dataset, table):
        self.project = project
        self.dataset = dataset
        self.table = table
        

    def process(self, period):
        from google.cloud import bigquery
        client = bigquery.Client()
        query = f"""
        DELETE FROM `{self.project}.{self.dataset}.{self.table}`
        WHERE periodo = @period
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("period", "STRING", period)
            ]
        )
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        yield period
        
def run_pipeline():
    
    os.environ["ENVIRONMENT"] = "development"
    environment = os.environ.get("ENVIRONMENT", "development")

    project_template = "sura-pe-{env}-analitica"
    input_table_template = "{project}.dataset_marketing_ltv.tbl_mkt"
    output_table_template = "{project}.dataset_marketing_ltv.tbl_mkt_output"
    bucket_template = "gs://sura-pe-{env}-analitica-dataflow-files/LTV"

    if environment == "development":
        env = "dev"
    elif environment == "test":
        env = "test"
    elif environment == "production":
        env = "prod"
    else:
        raise ValueError("Entorno no válido")
    
    project_id = project_template.format(env=env)
    input_table = input_table_template.format(project=project_id)
    output_table = output_table_template.format(project=project_id)
    temp_location = bucket_template.format(env=env)
    
    options = PipelineOptions(
        runner='DataflowRunner',
        project=project_id,
        region='us-central1', 
        temp_location=temp_location
    )

    with beam.Pipeline(options=options) as pipeline:
        # Step to read a single 'periodo' value from the input table
        periods = (
            pipeline
            | 'ReadPeriodFromBigQuery' >> ReadFromBigQuery(
                query=f'SELECT periodo FROM `{input_table}` LIMIT 1',
                use_standard_sql=True)
            | 'ExtractPeriod' >> beam.ParDo(ExtractPeriod())
        )

        delete_result = ( periods | 'DeleteFromBigQuery' >> beam.ParDo(DeleteFromBigQuery(
            project=project_id,
            dataset='dataset_marketing_ltv',  # Replace with the correct dataset
            table='tbl_mkt_output'   ))
            )
         
        deletion_done = (
            delete_result
            | 'DeletionComplete' >> beam.Map(lambda x: None)
        )
         
        # Step to read the input table for processing
        input_data = (
            pipeline
            | 'ReadFromBigQueryForProcessing' >> beam.io.ReadFromBigQuery(table=input_table)
        )

        # Process the input data
        processed_data = (
            input_data
            | 'WaitForDeletion' >> beam.FlatMap(lambda x, _: [x], beam.pvalue.AsList(deletion_done))
            | 'ProcessElements' >> beam.ParDo(CalculoLifeTimeValue())
        )

        # Write the processed data to the output table
        processed_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            output_table,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    run_pipeline()