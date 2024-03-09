using System.Data;

namespace Model.Query
{
    public interface IDbQuery
    {

        /// <summary>
        /// Funcion para traer el listado de los anticipos correspondientes a los documentos que se procesaran, 
        /// el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es uno de los Anticipos </returns>
        DataTable GetAnticiposData();

        /// <summary>
        /// Funcion para traer el listado de los Descuentos correspondientes a los documentos que se procesaran, 
        /// el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es uno de los Descuentos  </returns>
        DataTable GetDescuentosData();

        /// <summary>
        /// Funcion para traer el listado de los documentos que se van a procesar, el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es un documento </returns>
        DataTable GetDocumentsData();

        /// <summary>
        /// Funcion para traer el listado de los impuestos correspondientes a los documentos que se procesaran, 
        /// el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es uno de los Impuestos </returns>
        DataTable GetImpuestosData();

        /// <summary>
        /// Funcion para traer el listado de las retenciones correspondientes a los documentos que se procesaran, 
        /// el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es uno de las Retenciones </returns>
        DataTable GetDatosExtraData();

        /// <summary>
        /// Funcion para traer el listado de los anticipos correspondientes a los documentos que se procesaran, 
        /// el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es uno de los Anticipos </returns>
        DataTable GetItemsData();

        /// <summary>
        /// Funcion para traer el listado de los anticipos correspondientes a los documentos que se procesaran, 
        /// el orden de los campos puede varias pero no se puede cambiar el nombre ni disminuir la cantidad
        /// </summary>
        /// <returns> Devuelve una datatable. Columnas son los campos del query y cada fila es uno de los Adjuntos </returns>
        DataTable GetAdjuntosData();

        /// <summary>
        /// Funcion para insertar en la tabla de traza los documentos que se actualizaron 
        /// </summary>
        /// <param name="DocNum">Numero de documento a insertar</param>
        /// <param name="idDocumentoElectronico">Id del documento devuelto por el servicio web</param>
        /// <param name="cufe">valor Cufe a insertar en la tabla</param>
        /// <param name="qr_data">data devuelta por el servicio web</param>
        /// <returns> Void </returns>
        void InsertSuccessData(string DocNum, string idDocumentoElectronico, string cufe, string qr_data);
        void InsertFailData(string DocNum, string numDoc, string log, string estado);
        DataTable GetSeriesName(string DocNum);
        void DeleteRowsM();
    }
}