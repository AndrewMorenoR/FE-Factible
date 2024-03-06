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
	public class AnticiposGeneration : IAnticiposGeneration
	{
		private readonly IDbQuery dbQuery;
		private readonly IEventLogStore CsvGeneratorLog;

		public AnticiposGeneration(IDbQuery dbQuery, IEventLogStore csvGeneratorLog)
		{
			this.dbQuery = dbQuery;
			CsvGeneratorLog = csvGeneratorLog;
		}

		/// <summary>
		/// Genera un listado de objetos del tipo XmlAnticipo tomando como entrada la respuesta del query
		/// </summary>
		/// <returns> Devuelve un listado de objetos XmlAnticipo los cuales son los anticipos asignados a los documentos a procesar  </returns>
		public List<XmlAnticipo> GenerateAnticiposList()
		{
			try
			{
				DataTable AnticiposTable = dbQuery.GetAnticiposData();
				return GenerateList(AnticiposTable);
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateAnticiposList  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// Convierte la informacion de anticipos en una dataTable en un listado de objetos 
		/// </summary>
		/// <param name="AnticiposTable">Recibe una data table con la informacion de anticipos</param>
		/// <returns> Devuelve un listado de objetos XmlAnticipo </returns>
		private List<XmlAnticipo> GenerateList(DataTable AnticiposTable)
		{
			try
			{
				//Se crea un nuevo objeto para almacenar la informacion
				List<XmlAnticipo> AnticipoList = new List<XmlAnticipo>();

				if (AnticiposTable != null)
				{
					XmlAnticipo Anticipo;
					foreach (DataRow drow in AnticiposTable.Rows)
					{
						//Se genera un objeto y se le asigna la informacion de un anticipo
						Anticipo = new XmlAnticipo()
						{
							DOCNUM = drow["DOCNUM"].ToString(),
							identificador = drow["ANTICIPO_identificador"].ToString(),
							valor = drow["ANTICIPO_valor"].ToString(),
							fecha = drow["ANTICIPO_fecha"].ToString()
						};
						//se agrega el anticipo al listado
						AnticipoList.Add(Anticipo);
					}
				}

				return AnticipoList;
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateList  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}
	}
}
