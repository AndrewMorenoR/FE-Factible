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
	public class ImpuestosGeneration : IImpuestosGeneration
	{
		private readonly IDbQuery dbQuery;
		private readonly IEventLogStore CsvGeneratorLog;

		public ImpuestosGeneration(IDbQuery dbQuery, IEventLogStore csvGeneratorLog)
		{
			this.dbQuery = dbQuery;
			CsvGeneratorLog = csvGeneratorLog;
		}

		/// <summary>
		/// Genera un listado de objetos del tipo XmlImpuesto tomando como entrada la respuesta del query
		/// </summary>
		/// <returns> Devuelve un listado de objetos XmlImpuesto los cuales son los Impuestos asignados a los documentos a procesar  </returns>
		public List<XmlImpuesto> GenerateImpuestosList()
		{
			try
			{
				DataTable ImpuestosTable = dbQuery.GetImpuestosData();
				return GenerateList(ImpuestosTable);
			}
			catch (Exception exp)
			{
				return null;
			}
		}

		/// <summary>
		/// Convierte la informacion de Impuestos en una dataTable en un listado de objetos 
		/// </summary>
		/// <param name="ImpuestosTable">Recibe una data table con la informacion de Impuestos</param>
		/// <returns> Devuelve un listado de objetos XmlImpuesto </returns>
		private List<XmlImpuesto> GenerateList(DataTable ImpuestosTable)
		{
			try
			{
				//Se crea un nuevo objeto para almacenar la informacion
				List<XmlImpuesto> ImpuestosList = new List<XmlImpuesto>();

				if (ImpuestosTable != null)
				{
					XmlImpuesto Impuesto;
					foreach (DataRow drow in ImpuestosTable.Rows)
					{
						if (drow["IMPUESTOS_factor"].ToString() != "0.00")
						{
							//Se genera un objeto y se le asigna la informacion de n Impuesto
							Impuesto = new XmlImpuesto()
							{
								DOCNUM = drow["DOCNUM"].ToString(),
								idimpuesto = drow["IMPUESTOS_idimpuesto"].ToString(),
								baseImp = drow["IMPUESTOS_base"].ToString(),
								factor = drow["IMPUESTOS_factor"].ToString(),
								estarifaunitaria = drow["IMPUESTOS_estarifaunitaria"].ToString(),
								valor = drow["IMPUESTOS_valor"].ToString()
							};
							//se agrega el impuesto al listado
							ImpuestosList.Add(Impuesto);
						}
					}
				}

				return ImpuestosList;
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateList  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}
	}
}
