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
	public class DescuentosGeneration : IDescuentosGeneration
	{
		private readonly IDbQuery dbQuery;
		private readonly IEventLogStore CsvGeneratorLog;

		public DescuentosGeneration(IDbQuery dbQuery, IEventLogStore csvGeneratorLog)
		{
			this.dbQuery = dbQuery;
			CsvGeneratorLog = csvGeneratorLog;
		}

		/// <summary>
		/// Genera un listado de objetos del tipo XmlCargo tomando como entrada la respuesta del query de Descuentos
		/// </summary>
		/// <returns> Devuelve un listado de objetos XmlCargo los cuales son los descuentos asignados a los documentos a procesar  </returns>
		public List<XmlCargo> GenerateDescuentosList()
		{
			try
			{
				DataTable DescuentosTable = dbQuery.GetDescuentosData();
				return GenerateList(DescuentosTable);
			}
			catch (Exception exp)
			{
				return null;
			}
		}

		/// <summary>
		/// Convierte la informacion de Descuentos en una dataTable en un listado de objetos 
		/// </summary>
		/// <param name="DescuentosTable">Recibe una data table con la informacion de Descuentos</param>
		/// <returns> Devuelve un listado de objetos XmlCargo </returns>
		private List<XmlCargo> GenerateList(DataTable DescuentosTable)
		{
			try
			{
				//Se crea un nuevo objeto para almacenar la informacion
				List<XmlCargo> DescuentoList = new List<XmlCargo>();

				if (DescuentosTable != null)
				{
					XmlCargo Descuento;
					foreach (DataRow drow in DescuentosTable.Rows)
					{
						if (drow["DESC_porcentaje"].ToString() != "0.000000")
						{
							//Se genera un objeto y se le asigna la informacion de un anticipo
							Descuento = new XmlCargo()
							{
								DOCNUM = drow["DOCNUM"].ToString(),
								idconcepto = drow["DESC_idconcepto"].ToString(),
								escargo = drow["DESC_escargo"].ToString(),
								descripcion = drow["DESC_descripcion"].ToString(),
								porcentaje = drow["DESC_porcentaje"].ToString(),
								baseCargo = drow["DESC_base"].ToString(),
								valor = drow["DESC_valor"].ToString(),
							};
							//se agrega el anticipo al listado
							DescuentoList.Add(Descuento);
						}
					}
				}
				return DescuentoList;
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateList  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}
	}
}
