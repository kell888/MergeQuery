namespace Test
{
    partial class Form1
    {
        /// <summary>
        /// 必需的设计器变量。
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// 清理所有正在使用的资源。
        /// </summary>
        /// <param name="disposing">如果应释放托管资源，为 true；否则为 false。</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows 窗体设计器生成的代码

        /// <summary>
        /// 设计器支持所需的方法 - 不要
        /// 使用代码编辑器修改此方法的内容。
        /// </summary>
        private void InitializeComponent()
        {
            this.button1 = new System.Windows.Forms.Button();
            this.tb_tablenames = new System.Windows.Forms.TextBox();
            this.textBox2 = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.tb_selectFields = new System.Windows.Forms.TextBox();
            this.label3 = new System.Windows.Forms.Label();
            this.tb_timeField = new System.Windows.Forms.TextBox();
            this.label4 = new System.Windows.Forms.Label();
            this.tb_where = new System.Windows.Forms.TextBox();
            this.label5 = new System.Windows.Forms.Label();
            this.tb_orderby = new System.Windows.Forms.TextBox();
            this.button2 = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // button1
            // 
            this.button1.Location = new System.Drawing.Point(435, 108);
            this.button1.Name = "button1";
            this.button1.Size = new System.Drawing.Size(75, 31);
            this.button1.TabIndex = 0;
            this.button1.Text = "GetSQL";
            this.button1.UseVisualStyleBackColor = true;
            this.button1.Click += new System.EventHandler(this.button1_Click);
            // 
            // tb_tablenames
            // 
            this.tb_tablenames.Location = new System.Drawing.Point(66, 10);
            this.tb_tablenames.Name = "tb_tablenames";
            this.tb_tablenames.Size = new System.Drawing.Size(346, 21);
            this.tb_tablenames.TabIndex = 1;
            this.tb_tablenames.Text = "v_train_log";
            // 
            // textBox2
            // 
            this.textBox2.Location = new System.Drawing.Point(12, 155);
            this.textBox2.Multiline = true;
            this.textBox2.Name = "textBox2";
            this.textBox2.Size = new System.Drawing.Size(498, 144);
            this.textBox2.TabIndex = 2;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(13, 13);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(47, 12);
            this.label1.TabIndex = 3;
            this.label1.Text = "表/视图";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(13, 40);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(53, 12);
            this.label2.TabIndex = 5;
            this.label2.Text = "要查字段";
            // 
            // tb_selectFields
            // 
            this.tb_selectFields.Location = new System.Drawing.Point(66, 37);
            this.tb_selectFields.Name = "tb_selectFields";
            this.tb_selectFields.Size = new System.Drawing.Size(346, 21);
            this.tb_selectFields.TabIndex = 4;
            this.tb_selectFields.Text = "top 20 *";
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(13, 67);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(53, 12);
            this.label3.TabIndex = 7;
            this.label3.Text = "时间字段";
            // 
            // tb_timeField
            // 
            this.tb_timeField.Location = new System.Drawing.Point(66, 64);
            this.tb_timeField.Name = "tb_timeField";
            this.tb_timeField.Size = new System.Drawing.Size(346, 21);
            this.tb_timeField.TabIndex = 6;
            this.tb_timeField.Text = "pass_time";
            // 
            // label4
            // 
            this.label4.AutoSize = true;
            this.label4.Location = new System.Drawing.Point(13, 94);
            this.label4.Name = "label4";
            this.label4.Size = new System.Drawing.Size(53, 12);
            this.label4.TabIndex = 9;
            this.label4.Text = "查询条件";
            // 
            // tb_where
            // 
            this.tb_where.Location = new System.Drawing.Point(66, 91);
            this.tb_where.Name = "tb_where";
            this.tb_where.Size = new System.Drawing.Size(346, 21);
            this.tb_where.TabIndex = 8;
            // 
            // label5
            // 
            this.label5.AutoSize = true;
            this.label5.Location = new System.Drawing.Point(13, 121);
            this.label5.Name = "label5";
            this.label5.Size = new System.Drawing.Size(53, 12);
            this.label5.TabIndex = 11;
            this.label5.Text = "排序方法";
            // 
            // tb_orderby
            // 
            this.tb_orderby.Location = new System.Drawing.Point(66, 118);
            this.tb_orderby.Name = "tb_orderby";
            this.tb_orderby.Size = new System.Drawing.Size(346, 21);
            this.tb_orderby.TabIndex = 10;
            this.tb_orderby.Text = "order by id desc";
            // 
            // button2
            // 
            this.button2.Location = new System.Drawing.Point(435, 10);
            this.button2.Name = "button2";
            this.button2.Size = new System.Drawing.Size(75, 31);
            this.button2.TabIndex = 12;
            this.button2.Text = "CanConnect";
            this.button2.UseVisualStyleBackColor = true;
            this.button2.Click += new System.EventHandler(this.button2_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 12F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(523, 311);
            this.Controls.Add(this.button2);
            this.Controls.Add(this.label5);
            this.Controls.Add(this.tb_orderby);
            this.Controls.Add(this.label4);
            this.Controls.Add(this.tb_where);
            this.Controls.Add(this.label3);
            this.Controls.Add(this.tb_timeField);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.tb_selectFields);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.textBox2);
            this.Controls.Add(this.tb_tablenames);
            this.Controls.Add(this.button1);
            this.Name = "Form1";
            this.Text = "测试GetQuerySQL";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.TextBox tb_tablenames;
        private System.Windows.Forms.TextBox textBox2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox tb_selectFields;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox tb_timeField;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.TextBox tb_where;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.TextBox tb_orderby;
        private System.Windows.Forms.Button button2;
    }
}

