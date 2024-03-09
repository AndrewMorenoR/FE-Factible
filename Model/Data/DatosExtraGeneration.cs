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
						if (drow["RETENCIONES_valor"].ToString() != null)
						{
							/*XmlValor valor = new XmlValor();
							XmlColumnas columnas = new XmlColumnas();
							columnas.columna = drow["RETENCIONES_valor"].ToString();
							valor.columnas = columnas;
							*/
							string clv = drow["RETENCIONES_tipo"].ToString();

							//CsvGeneratorLog.StoreLog("Factura: " + drow["DOCNUM"].ToString() + " clave: " + clv + " valor: " + drow["RETENCIONES_valor"].ToString(), EventLogEntryType.Information);
							if (clv.Equals("0"))
							{
								clv = "AUTORETENCION";
							}
							if (clv.Equals("1"))
							{
								clv = "RETEFUENTE";
							}
							if (clv.Equals("3"))
							{
								clv = "RETEIVA";
							}
							if (clv.Equals("4"))
							{
								clv = "RTEICA";
							}
                            if (clv.Equals("9"))
                            {
                                clv = "CONTACTO_PERSONA";
                            }
                            if (clv.Equals("10"))
                            {
                                clv = "DESPACHO_NOMBRE";
                            }
                            if (clv.Equals("11"))
                            {
                                clv = "DESPACHO_IDENTIFICACION";
                            }
                            if (clv.Equals("12"))
                            {
                                clv = "DESPACHO_DIRECCION";
                            }
                            if (clv.Equals("13"))
                            {
                                clv = "DESPACHO_CIUDAD";
                            }
                            if (clv.Equals("14"))
                            {
                                clv = "DESPACHO_TELEFONOS";
                            }
                            if (clv.Equals("15"))
                            {
                                clv = "DESPACHO_CONTACTO";
                            }
                            //Nuevos datos extra  MARCA DE EMBARQUE:
                            if (clv.Equals("16"))
                            {
                                clv = "EMBARQUE_EMPRESA";
                            }
                            if (clv.Equals("17"))
                            {
                                clv = "EMBARQUE_DIRECCION";
                            }
                            if (clv.Equals("18"))
                            {
                                clv = "EMBARQUE_CORREO";
                            }
                            if (clv.Equals("19"))
                            {
                                clv = "EMBARQUE_TELEFONO";
                            }
                            if (clv.Equals("20"))
                            {
                                clv = "EMBARQUE_CONTACTO";
                            }
                            if (clv.Equals("21"))
                            {
                                clv = "EMBARQUE_IDENTIFICACION";
                            }
                            if (clv.Equals("22"))
                            {
                                clv = "MODALIDAD_EXPORTACION";
                            }
                            //Nuevos datos extra  MARCA DE EMBARQUE:

                            if (clv.Equals("23"))
                            {
                                clv = "CREDIT";
                            }
                            if (clv.Equals("24"))
                            {
                                clv = "EMBARQUE_CONDICION";
                            }
                            if (clv.Equals("25"))
                            {
                                clv = "NEGOCIACION";
                            }
                            if (clv.Equals("26"))
                            {
                                clv = "TRANSPORTE";
                            }
                            if (clv.Equals("27"))
                            {
                                clv = "EMBALAJE";
                            }
                            if (clv.Equals("28"))
                            {
                                clv = "FABRICANTE_PAIS";
                            }
                            if (clv.Equals("29"))
                            {
                                clv = "ORIGEN";
                            }
                            if (clv.Equals("30"))
                            {
                                clv = "DESTINO";
                            }
                            if (clv.Equals("31"))
                            {
                                clv = "PESO_NETO";
                            }
                            if (clv.Equals("32"))
                            {
                                clv = "PESO_BRUTO";
                            }
                            if (clv.Equals("33"))
                            {
                                clv = "ARANCEL";
                            }
                            if (clv.Equals("34"))
                            {
                                clv = "PAQUETE";
                            }
                            if (clv.Equals("35"))
                            {
                                clv = "EMBARQUE";
                            }
                            if (clv.Equals("36"))
                            {
                                clv = "FLETE";
                            }
                            if (clv.Equals("37"))
                            {
                                clv = "SEGURO";
                            }

                            //Se genera un objeto y se le asigna la informacion de una Retención

                            DatoExtra = new XmlDatoExtra()
							{
								DOCNUM = drow["DOCNUM"].ToString(),
								clave = clv,
								tipo = drow["RETENCIONES_idretencion"].ToString(),
								valor = drow["RETENCIONES_valor"].ToString()
							};

							//se agrega el impuesto al listado
							DatosExtraList.Add(DatoExtra);
						}
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
