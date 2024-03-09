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
	public class GeneralDataGeneration : IGeneralDataGeneration
	{
		private readonly IDbQuery dbQuery;
		private readonly IEventLogStore CsvGeneratorLog;
		private readonly IAnticiposGeneration anticiposGeneration;
		private readonly IDescuentosGeneration descuentosGeneration;
		private readonly IImpuestosGeneration impuestosGeneration;
		private readonly IItemsGeneration itemsGeneration;
		private readonly IAdjuntosGeneration attachmentsGeneration;
		private readonly IDatosExtraGeneration datosExtraGeneration;

		public GeneralDataGeneration(IDbQuery dbQuery, IEventLogStore csvGeneratorLog, IAnticiposGeneration anticiposGeneration, IDescuentosGeneration descuentosGeneration, IImpuestosGeneration impuestosGeneration, IItemsGeneration itemsGeneration, IAdjuntosGeneration attachmentsGeneration, IDatosExtraGeneration datosExtraGeneration)
		{
			this.dbQuery = dbQuery;
			CsvGeneratorLog = csvGeneratorLog;
			this.anticiposGeneration = anticiposGeneration;
			this.descuentosGeneration = descuentosGeneration;
			this.impuestosGeneration = impuestosGeneration;
			this.itemsGeneration = itemsGeneration;
			this.attachmentsGeneration = attachmentsGeneration;
			this.datosExtraGeneration = datosExtraGeneration;
		}

		/// <summary>
		/// Genera un listado de objetos del tipo Xmldocumento tomando como entrada la respuesta del query de documentos, y los listados de anticipos, Descuentos, Impuestos e Items
		/// </summary>
		/// <returns> Devuelve un listado de objetos Xmldocumento los cuales son los documentos a procesar  </returns>
		public List<Xmldocumento> GenerateDocumentList()
		{
			try
			{
				DataTable DocumentTable = dbQuery.GetDocumentsData();
				List<XmlAnticipo> anticipos = anticiposGeneration.GenerateAnticiposList();
				List<XmlCargo> cargos = descuentosGeneration.GenerateDescuentosList();
				List<XmlImpuesto> impuestos = impuestosGeneration.GenerateImpuestosList();
				List<XmlItem> items = itemsGeneration.GenerateItemsList();
				List<XmlAdjunto> adjuntos = attachmentsGeneration.GenerateAttachmentsList();
				List<XmlDatoExtra> datosExtra = datosExtraGeneration.GenerateDatosExtraList();
				return GenerateList(DocumentTable, anticipos, cargos, impuestos, items, adjuntos, datosExtra);
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateDocumentList  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// Convierte la informacion de anticipos en una dataTable en un listado de objetos 
		/// </summary>
		/// <param name="documentTable">Recibe una data table con la informacion de documentos</param>
		/// <param name="anticipos">Recibe un istado de anticipos</param>
		/// <param name="cargos">Recibe un istado de Descuentos</param>
		/// <param name="impuestos">Recibe un istado de Impuestos</param>
		/// <param name="items">Recibe un istado de Items</param>
		/// <returns> Devuelve un listado de objetos Xmldocumento </returns>
		private List<Xmldocumento> GenerateList(DataTable documentTable, List<XmlAnticipo> anticipos, List<XmlCargo> cargos, List<XmlImpuesto> impuestos, List<XmlItem> items, List<XmlAdjunto> attachments, List<XmlDatoExtra> datosExtra)
		{
			try
			{
				//Console.WriteLine("llegue hasta aqui");
				List<Xmldocumento> DocumentosList = new List<Xmldocumento>();

				if (documentTable != null)
				{
					Xmldocumento Documento;
					//Se recorre uno a uno los documentos a procesar
					foreach (DataRow drow in documentTable.Rows)
					{
						Documento = new Xmldocumento()
						{
							DOCNUM = drow["DocNum"].ToString(),
							idnumeracion = drow["idnumeracion"].ToString(),
							numero = drow["numero"].ToString(),
							idambiente = drow["idambiente"].ToString(),
							fechadocumento = drow["fechadocumento"].ToString(),
							fechavencimiento = drow["fechavencimiento"].ToString(),
							tipofactura = drow["tipofactura"].ToString(),
							tipooperacion = drow["tipooperacion"].ToString(),
							notas = drow["notas"].ToString(),
							//fechaimpuestos = drow["fechaimpuestos"].ToString(),
							moneda = drow["moneda"].ToString(),
							cufe = drow["cufe"].ToString(),
							idreporte = drow["idreporte"].ToString(),
							correoscopia = new XmlCorreosCopia() { correocopia = drow["correocopia"].ToString() },
							formaspago = GenerateFormaPago(drow),
							//periodofacturacion = new XmlPeriodoFacturacion() { fechafinal = drow["PF_fechafinal"].ToString(), fechainicial = drow["PF_fechainicial"].ToString() },
							idconceptonota = drow["idconceptonota"].ToString(),
							referencias = GenerateReferencias(drow),
							tasasdecambio = GenerateTasasDeCambio(drow),
							entrega = GenerateEntrega(drow),
							facturador = GenerarFacturador(drow),
							adquiriente = GenerarAdquiriente(drow),
							//Se asigna a impuetos los impuestos correspondientes al documento que se esta procesando
							impuestos = new XmlImpuestos() { impuesto = impuestos.Where(x => x.DOCNUM == drow["DocNum"].ToString()).ToList() },
							//Se asigna a anticipos los anticipos correspondientes al documento que se esta procesando
							anticipos = new XmlAnticipos() { anticipo = anticipos.Where(x => x.DOCNUM == drow["DocNum"].ToString()).ToList() },
							//Se asigna a cargos los descuentos correspondientes al documento que se esta procesando
							cargos = new XmlCargos() { cargo = cargos.Where(x => x.DOCNUM == drow["DocNum"].ToString()).ToList() },
							totales = GenerarTotales(drow),
							//Se asigna a items los items correspondientes al documento que se esta procesando
							items = new XmlItems() { item = items.Where(x => x.DOCNUM == drow["DocNum"].ToString()).ToList() },
							//Se asigna a Adjuntos los items correspondientes al documento que se esta procesando
							adjuntos = new XmlAdjuntos() { adjunto = attachments.Where(x => x.DOCNUM == drow["DocNum"].ToString()).ToList() },
							//Se asigna el nodo para la oden de compra relacionada en la factura
							ordendecompra = GenerarOrdenCompra(drow),
							//Se asignan los datos extra a nivel de documento
							datosextra = new XmlDatosExtra() { datoextra = datosExtra.Where(x => x.DOCNUM == drow["DocNum"].ToString() && x.clave != "AUTO").ToList() }
						};
						DocumentosList.Add(Documento);
					}
				}

				return DocumentosList;
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateList  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlTotales con la informacion correspondiente al documentos, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlTotales </returns>
		private XmlTotales GenerarTotales(DataRow drow)
		{
			try
			{
				return new XmlTotales()
				{
					totalbruto = drow["TOTAL_totalbruto"].ToString(),
					baseimponible = drow["TOTAL_baseimponible"].ToString(),
					totalbrutoconimpuestos = drow["TOTAL_totalbrutoconimpuestos"].ToString(),
					totaldescuento = drow["TOTAL_totaldescuento"].ToString(),
					totalcargos = drow["TOTAL_totalcargos"].ToString(),
					totalanticipos = drow["TOTAL_totalanticipos"].ToString(),
					totalapagar = drow["TOTAL_totalapagar"].ToString(),
					payableroundingamount = drow["TOTAL_payableroundingamount"].ToString(),
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerarTotales  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlTotales con la informacion correspondiente al documentos, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlTotales </returns>
		private XmlOrdenDeCompra GenerarOrdenCompra(DataRow drow)
		{
			try
			{
				return new XmlOrdenDeCompra()
				{
					codigo = drow["ORDENC_codigo"].ToString(),
					fechageneracion = " ",
					base64 = " ",
					nombrearchivo = " ",
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerarTotales  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlAdquiriente con la informacion correspondiente al documentos, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlAdquiriente </returns>
		private XmlAdquiriente GenerarAdquiriente(DataRow drow)
		{
			try
			{
				return new XmlAdquiriente()
				{
					idtipopersona = drow["ADQUIR_idtipopersona"].ToString(),
					idactividadeconomica = drow["ADQUIR_idactividadeconomica"].ToString(),
					nombrecomercial = drow["ADQUIR_nombrecomercial"].ToString(),
					idciudad = drow["ADQUIR_idciudad"].ToString(),
					direccion = drow["ADQUIR_direccion"].ToString(),
					codigopostal = drow["ADQUIR_codigopostal"].ToString(),
					nombres = drow["ADQUIR_nombres"].ToString(),
					apellidos = drow["ADQUIR_apellidos"].ToString(),
					idtipodocumentoidentidad = drow["ADQUIR_idtipodocumentoidentidad"].ToString(),
					identificacion = drow["ADQUIR_identificacion"].ToString(),
					digitoverificacion = drow["ADQUIR_digitoverificacion"].ToString(),
					idtiporegimen = drow["ADQUIR_idtiporegimen"].ToString(),
					direccionfiscal = new XmlDireccionFiscal() { idciudad = drow["ADQUIR_DIR_idciudad"].ToString(), direccion = drow["ADQUIR_DIR_direccion"].ToString(), codigopostal = drow["ADQUIR_DIR_codigopostal"].ToString() },
					matriculamercantil = drow["ADQUIR_matriculamercantil"].ToString(),
					emailcontacto = drow["ADQUIR_emailcontacto"].ToString(),
					emailentrega = drow["ADQUIR_emailentrega"].ToString(),
					telefono = drow["ADQUIR_telefono"].ToString(),
					obligaciones = drow["ADQUIR_obligaciones"].ToString(),
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerarAdquiriente  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlFacturador con la informacion correspondiente al documentos, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlFacturador </returns>
		private XmlFacturador GenerarFacturador(DataRow drow)
		{
			try
			{
				return new XmlFacturador()
				{
					idtipopersona = drow["FACT_idtipopersona"].ToString(),
					idactividadeconomica = drow["FACT_idactividadeconomica"].ToString(),
					nombrecomercial = drow["FACT_nombrecomercial"].ToString(),
					idciudad = drow["FACT_idciudad"].ToString(),
					direccion = drow["FACT_direccion"].ToString(),
					codigopostal = drow["FACT_codigopostal"].ToString(),
					nombres = drow["FACT_nombres"].ToString(),
					apellidos = drow["FACT_apellidos"].ToString(),
					idtipodocumentoidentidad = drow["FACT_idtipodocumentoidentidad"].ToString(),
					identificacion = drow["FACT_identificacion"].ToString(),
					digitoverificacion = drow["FACT_digitoverificacion"].ToString(),
					idtiporegimen = drow["FACT_idtiporegimen"].ToString(),
					direccionfiscal = new XmlDireccionFiscal() { idciudad = drow["FACT_DIR_idciudad"].ToString(), direccion = drow["FACT_DIR_direccion"].ToString(), codigopostal = drow["FACT_DIR_codigopostal"].ToString() },
					matriculamercantil = drow["FACT_matriculamercantil"].ToString(),
					emailcontacto = drow["FACT_emailcontacto"].ToString(),
					emailentrega = drow["FACT_emailentrega"].ToString(),
					telefono = drow["FACT_telefono"].ToString(),
					obligaciones = drow["FACT_obligaciones"].ToString(),
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerarFacturador  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlEntrega con la informacion correspondiente al documentos, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlEntrega </returns>
		private XmlEntrega GenerateEntrega(DataRow drow)
		{
			try
			{
				return new XmlEntrega()
				{
					fecha = drow["ENT_fecha"].ToString(),
					idciudad = drow["ENT_idciudad"].ToString(),
					direccion = drow["ENT_direccion"].ToString(),
					codigopostal = drow["ENT_codigopostal"].ToString(),
					transportador = GenerarTransportador(drow),
					metododepago = drow["ENT_metododepago"].ToString(),
					condicionesdeentrega = drow["ENT_condicionesdeentrega"].ToString()
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateEntrega  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlTransportador con la informacion correspondiente al documentos, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlTransportador </returns>
		private XmlTransportador GenerarTransportador(DataRow drow)
		{
			try
			{
				return new XmlTransportador()
				{
					idtipopersona = drow["ENT_TRANS_idtipopersona"].ToString(),
					idactividadeconomica = drow["ENT_TRANS_idactividadeconomica"].ToString(),
					nombrecomercial = drow["ENT_TRANS_nombrecomercial"].ToString(),
					idciudad = drow["ENT_TRANS_idciudad"].ToString(),
					direccion = drow["ENT_TRANS_direccion"].ToString(),
					codigopostal = drow["ENT_TRANS_codigopostal"].ToString(),
					nombres = drow["ENT_TRANS_nombres"].ToString(),
					apellidos = drow["ENT_TRANS_apellidos"].ToString(),
					idtipodocumentoidentidad = drow["ENT_TRANS_idtipodocumentoidentidad"].ToString(),
					identificacion = drow["ENT_TRANS_identificacion"].ToString(),
					digitoverificacion = drow["ENT_TRANS_digitoverificacion"].ToString(),
					idtiporegimen = drow["ENT_TRANS_idtiporegimen"].ToString(),
					direccionfiscal = new XmlDireccionFiscal() { idciudad = drow["ENT_TRANSDF_Idciudad"].ToString(), direccion = drow["ENT_TRANSDF_direccion"].ToString(), codigopostal = drow["ENT_TRANSDF_codigopostal"].ToString() },
					matriculamercantil = drow["ENT_TRANS_matriculamercantil"].ToString(),
					emailcontacto = drow["ENT_TRANS_emailcontacto"].ToString(),
					emailentrega = drow["ENT_TRANS_emailentrega"].ToString(),
					telefono = drow["ENT_TRANS_telefono"].ToString(),
					obligaciones = drow["ENT_TRANS_obligaciones"].ToString()
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerarTransportador  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlReferencias con la informacion correspondiente al documentos, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlReferencias </returns>
		private XmlReferencias GenerateReferencias(DataRow drow)
		{
			try
			{
				return new XmlReferencias()
				{
					referencia = new XmlReferencia()
					{
						idnumeracion = drow["REF_idnumeracion"].ToString(),
						numero = drow["REF_numero"].ToString()
					},
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateReferencias  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}


		/// <summary>
		/// se general un objeto del tipo XmlReferencias con la informacion correspondiente al documentos, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlTasaDeCabio </returns>
		private XmlTasasdecambio GenerateTasasDeCambio(DataRow drow)
		{
			try
			{
				return new XmlTasasdecambio()
				{
					tasadecambio = new XmlTasadecambio()
					{
						fecha = drow["TDC_fecha"].ToString(),
						moneda = drow["TDC_moneda"].ToString(),
						cambiominimo = drow["TDC_cambiominimo"].ToString(),
						tasarepresentativa = drow["TDC_tasarepresentativa"].ToString()
					},
				};
				//if (!drow["moneda"].ToString().Equals("COP"))
				//{

				//}

			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateTasasDeCambio  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}

		/// <summary>
		/// se general un objeto del tipo XmlFormasPago con la informacion correspondiente al documentos, extrae la informacion de la datarow
		/// </summary>
		/// <param name="drow">Recibe una data row con la informacion del documento</param>
		/// <returns> Devuelve un objeto XmlFormasPago </returns>
		private XmlFormasPago GenerateFormaPago(DataRow drow)
		{
			try
			{
				return new XmlFormasPago()
				{
					FormaPago = new XmlFormaPago()
					{
						idmetodopago = drow["FP_idmetodopago"].ToString(),
						idmediopago = drow["FP_idmediopago"].ToString(),
						fechavencimiento = drow["FP_fechavencimiento"].ToString(),
						identificador = drow["FP_identificador"].ToString(),
						dias = drow["FP_dias"].ToString()
					}
				};
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateFormaPago  {exp.Message}", EventLogEntryType.Error);
				return null;
			}
		}
	}
}
