using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using MergeQueryUtil;

namespace Test
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            string tablenames = tb_tablenames.Text;
            string selectFields = tb_selectFields.Text;
            string timeField = tb_timeField.Text;
            string where = tb_where.Text;
            string orderby = tb_orderby.Text;
            if (orderby != "")
                orderby = " " + orderby;
            string sql = MergeSQLQuery.GetQuerySQL(tablenames, selectFields, timeField, where, "connString") + orderby;
            textBox2.Text = sql;
        }

        private void button2_Click(object sender, EventArgs e)
        {
            SqlHelper sql = new SqlHelper("connString");
            bool flag = sql.CanConnect;
            if (flag)
                MessageBox.Show("Connected!");
            else
                MessageBox.Show("Can not connected!");
        }
    }
}
