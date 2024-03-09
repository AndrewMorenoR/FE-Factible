using Log;
using Model.Query;
using Model.XmlModel;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Model.Data
{
    public class AdjuntosGeneration : IAdjuntosGeneration
	{
		private readonly IDbQuery dbQuery;
		private readonly IEventLogStore CsvGeneratorLog;

		public AdjuntosGeneration(IDbQuery dbQuery, IEventLogStore csvGeneratorLog)
		{
			this.dbQuery = dbQuery;
			CsvGeneratorLog = csvGeneratorLog;
		}
		public List<XmlAdjunto> GenerateAttachmentsList()
		{
			try
			{
				DataTable AttachedTable = dbQuery.GetAdjuntosData();
				return GenerateList(AttachedTable);
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
		private List<XmlAdjunto> GenerateList(DataTable AttachedTable)
		{
			try
			{
				//Se crea un nuevo objeto para almacenar la informacion
				List<XmlAdjunto> AttachedList = new List<XmlAdjunto>();

				if (AttachedTable != null)
				{
					XmlAdjunto Item;
					foreach (DataRow drow in AttachedTable.Rows)
					{
						//Se genera un objeto y se le asigna la informacion de un item

						Item = new XmlAdjunto()
						{
							DOCNUM = drow["DOCNUM"].ToString(),
							codigo = drow["ANEXOS_codigo"].ToString(),
							base64 = Archivo(drow["ANEXOS_ruta"].ToString()),
							nombrearchivo = drow["ANEXOS_nombre"].ToString(),
							fechageneracion = HoraCreacionAdjunto(drow["ANEXOS_ruta"].ToString())
						};

						//se agrega el item al listado
						AttachedList.Add(Item);
					}
				}

				return AttachedList;
			}
			catch (Exception exp)
			{
				CsvGeneratorLog.StoreLog($"{this.ToString()}_GenerateList  {exp.Message}", EventLogEntryType.Error);
				return null;
			}

		}

		private string Archivo(string ruta)
		{
			string rutaArchivoPDF = ruta; // Ruta del archivo PDF
			string base64String = string.Empty;

			try
			{
				base64String = ConvertirPDFaBase64(rutaArchivoPDF);

			}
			catch (Exception ex)
			{
				Console.WriteLine("Error al convertir el archivo PDF a Base64: " + ex.Message);
				return base64String;
			}

			return base64String;
		}

		private string ConvertirPDFaBase64(string rutaArchivoPDF)
		{
			byte[] archivoBytes = File.ReadAllBytes(rutaArchivoPDF); // Leer bytes del archivo PDF
			string base64String = Convert.ToBase64String(archivoBytes); // Convertir bytes a Base64
			return base64String;
		}

		private string HoraCreacionAdjunto(string ruta)
        {
			string fecha = string.Empty;
            try
            {
				DateTime fileCreatedDate = File.GetLastWriteTime(ruta);
				fecha = fileCreatedDate.ToString("yyyy-MM-dd");
			}
            catch
            {
				return fecha;
            }
			return fecha;
		}
	}
}
