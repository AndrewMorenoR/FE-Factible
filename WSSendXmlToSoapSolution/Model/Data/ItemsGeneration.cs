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
	public class ItemsGeneration : IItemsGeneration
	{
		private readonly IDbQuery dbQuery;
		private readonly IEventLogStore CsvGeneratorLog;

		public ItemsGeneration(IDbQuery dbQuery, IEventLogStore csvGeneratorLog)
		{
			this.dbQuery = dbQuery;
			CsvGeneratorLog = csvGeneratorLog;
		}

		/// <summary>
		/// Genera un listado de objetos del tipo XmlItem tomando como entrada la respuesta del query
		/// </summary>
		/// <returns> Devuelve un listado de objetos XmlItem los cuales son los Items asignados a los documentos a procesar  </returns>
		public List<XmlItem> GenerateItemsList()
		{
			try
			{
				DataTable ItemsTable = dbQuery.GetItemsData();
				return GenerateList(ItemsTable);
			}
			catch (Exception exp)
			{
				return null;
			}
		}

		/// <summary>
		/// Convierte la informacion de items en una dataTable en un listado de objetos 
		/// </summary>
		/// <param name="ItemsTable">Recibe una data table con la informacion de items</param>
		/// <returns> Devuelve un listado de objetos XmlItem </returns>
		private List<XmlItem> GenerateList(DataTable ItemsTable)
		{
			try
			{
				//Se crea un nuevo objeto para almacenar la informacion
				List<XmlItem> ItemList = new List<XmlItem>();

				if (ItemsTable != null)
				{
					XmlItem Item;
					foreach (DataRow drow in ItemsTable.Rows)
					{
						//Se genera un objeto y se le asigna la informacion de un item

						if (drow["ITEMS_idmandante"].ToString() == "DS")
						{
							Item = new XmlItem()
							{
								DOCNUM = drow["DocNum"].ToString(),
								codigos = GenerateCodigos(drow),
								descripcion = drow["ITEMS_descripcion"].ToString(),
								notas = drow["ITEMS_notas"].ToString(),
								cantidad = drow["ITEMS_cantidad"].ToString(),
								cantidadporempaque = drow["ITEMS_cantidadporempaque"].ToString(),
								preciounitario = drow["ITEMS_preciounitario"].ToString(),
								unidaddemedida = drow["ITEMS_unidaddemedida"].ToString(),
								marca = drow["ITEMS_marca"].ToString(),
								modelo = drow["ITEMS_modelo"].ToString(),
								codigovendedor = drow["ITEMS_codigovendedor"].ToString(),
								subcodigovendedor = drow["ITEMS_subcodigovendedor"].ToString(),
								//			idmandante = drow["ITEMS_idmandante"].ToString(),
								regalo = drow["ITEMS_regalo"].ToString(),
								totalitem = drow["ITEMS_totalitem"].ToString(),
								fechacompra = drow["ITEMS_fechacompra"].ToString(),//DOCUMENTO SOPORTE
								formageneraciontransmision = drow["ITEMS_formageneraciontransmision"].ToString(),//DOCUMENTO SOPORTE
								cargos = GenerateCargo(drow),
								impuestos = GenerateImpuestos(drow),
								datosextra = GenerateDatoExtra(drow)
							};
						}
                        else
                        {
							Item = new XmlItem()
							{
								DOCNUM = drow["DocNum"].ToString(),
								codigos = GenerateCodigos(drow),
								descripcion = drow["ITEMS_descripcion"].ToString(),
								notas = drow["ITEMS_notas"].ToString(),
								cantidad = drow["ITEMS_cantidad"].ToString(),
								cantidadporempaque = drow["ITEMS_cantidadporempaque"].ToString(),
								preciounitario = drow["ITEMS_preciounitario"].ToString(),
								unidaddemedida = drow["ITEMS_unidaddemedida"].ToString(),
								marca = drow["ITEMS_marca"].ToString(),
								modelo = drow["ITEMS_modelo"].ToString(),
								codigovendedor = drow["ITEMS_codigovendedor"].ToString(),
								subcodigovendedor = drow["ITEMS_subcodigovendedor"].ToString(),
								//			idmandante = drow["ITEMS_idmandante"].ToString(),
								regalo = drow["ITEMS_regalo"].ToString(),
								totalitem = drow["ITEMS_totalitem"].ToString(),
								cargos = GenerateCargo(drow),
								impuestos = GenerateImpuestos(drow),
								datosextra = GenerateDatoExtra(drow)
							};
						}	
						
						//se agrega el item al listado
						ItemList.Add(Item);
					}
				}

				return ItemList;
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateList  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlDatosExtraItem con la informacion correspondiente al documento, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlDatosExtraItem </returns>
		private XmlDatosExtraItem GenerateDatoExtra(DataRow drow)
		{
			try
			{
				return new XmlDatosExtraItem()
				{
					datoextra = new XmlDatoExtraItem()
					{
						clave = drow["ITEMS_DT_clave"].ToString(),
						valor = drow["ITEMS_DT_valor"].ToString()
					}
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateDatoExtra  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlImpuestos con la informacion correspondiente al documento, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlImpuestos </returns>
		private XmlImpuestos GenerateImpuestos(DataRow drow)
		{
			try
			{
				List<XmlImpuesto> impuestiList = new List<XmlImpuesto>();
				if (drow["ITEMS_IMPUES_factor"].ToString() != "0.00")
				{
					impuestiList.Add(new XmlImpuesto()
					{
						idimpuesto = drow["ITEMS_IMPUES_idimpuesto"].ToString(),
						baseImp = drow["ITEMS_IMPUES_base"].ToString(),
						factor = drow["ITEMS_IMPUES_factor"].ToString(),
						estarifaunitaria = drow["ITEMS_IMPUES_estarifaunitaria"].ToString(),
						valor = drow["ITEMS_IMPUES_valor"].ToString(),
					});
				}
				return new XmlImpuestos()
				{
					impuesto = impuestiList
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateImpuestos  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlCargos con la informacion correspondiente al documento, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlCargos </returns>
		private XmlCargos GenerateCargo(DataRow drow)
		{
			try
			{
				List<XmlCargo> cargoList = new List<XmlCargo>();
				if (drow["ITEMS_CARGO_porcentaje"].ToString() != "0.0000")
				{
					cargoList.Add(new XmlCargo()
					{
						DOCNUM = drow["DocNum"].ToString(),
						idconcepto = drow["ITEMS_CARGO_idconcepto"].ToString(),
						escargo = drow["ITEMS_CARGO_escargo"].ToString(),
						descripcion = drow["ITEMS_CARGO_descripcion"].ToString(),
						porcentaje = drow["ITEMS_CARGO_porcentaje"].ToString(),
						baseCargo = drow["ITEMS_CARGO_base"].ToString(),
						valor = drow["ITEMS_CARGO_valor"].ToString()
					});
				}
				return new XmlCargos()
				{
					cargo = cargoList
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateCargo  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlCodigos con la informacion correspondiente al documento, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlCodigos </returns>
		private XmlCodigos GenerateCodigos(DataRow drow)
		{
			try
			{
				return new XmlCodigos()
				{
					estandar = new XmlEstandar()
					{
						idestandar = drow["ITEMS_COD_idestandar"].ToString(),
						nombreestandar = drow["ITEMS_COD_nombreestandar"].ToString(),
						codigo = drow["ITEMS_COD_codigo"].ToString()
					}
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateCodigos  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}
	}
}
