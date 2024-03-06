using Log;
using Model.Query;
using Model.XmlModel;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Model.Data
{
    public class DatosExtraGeneration : IDatosExtraGeneration
    {
		private readonly IDbQuery dbQuery;
		private readonly IEventLogStore CsvGeneratorLog;

		public DatosExtraGeneration(IDbQuery dbQuery, IEventLogStore csvGeneratorLog)
		{
			this.dbQuery = dbQuery;
			CsvGeneratorLog = csvGeneratorLog;
		}

		/// <summary>
		/// Genera un listado de objetos del tipo XmlImpuesto tomando como entrada la respuesta del query
		/// </summary>
		/// <returns> Devuelve un listado de objetos XmlImpuesto los cuales son los Impuestos asignados a los documentos a procesar  </returns>
		public List<XmlDatoExtra> GenerateDatosExtraList()
		{
			try
			{
				DataTable DatosExtraTable = dbQuery.GetDatosExtraData();
				return GenerateList(DatosExtraTable);
			}
			catch (Exception exp)
			{
				return null;
			}
		}

		/// <summary>
		/// Convierte la informacion de Impuestos en una dataTable en un listado de objetos 
		/// </summary>
		/// <param name="DatosExtraTable">Recibe una data table con la informacion de Impuestos</param>
		/// <returns> Devuelve un listado de objetos XmlImpuesto </returns>
		private List<XmlDatoExtra> GenerateList(DataTable DatosExtraTable)
		{
			try
			{
				//Se crea un nuevo objeto para almacenar la informacion
				List<XmlDatoExtra> DatosExtraList = new List<XmlDatoExtra>();

				if (DatosExtraTable != null)
				{
					XmlDatoExtra DatoExtra;
					
					foreach (DataRow drow in DatosExtraTable.Rows)
					{
						/*XmlValor valor = new XmlValor();
						XmlColumnas columnas = new XmlColumnas();
						columnas.columna = drow["RETENCIONES_valor"].ToString();
						valor.columnas = columnas;
						*/
						//Se genera un objeto y se le asigna la informacion de una Retención
						DatoExtra = new XmlDatoExtra()
						{
							DOCNUM = drow["DOCNUM"].ToString(),
							clave = drow["RETENCIONES_tipo"].ToString(),
							tipo = drow["RETENCIONES_idretencion"].ToString(),
							valor = drow["RETENCIONES_valor"].ToString()
						};
						//se agrega el impuesto al listado
						DatosExtraList.Add(DatoExtra);
					}
				}

				return DatosExtraList;
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateList  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}
	}
}
