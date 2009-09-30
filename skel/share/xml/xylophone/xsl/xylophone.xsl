<?xml version="1.0" encoding="utf-8"?>

<!--+
    | Copyright (c) 2008, Deutsches Elektronen-Synchrotron (DESY)
    | All rights reserved.
    | 
    | Redistribution and use in source and binary forms, with
    | or without modification, are permitted provided that the
    | following conditions are met:
    | 
    |   o  Redistributions of source code must retain the above
    |      copyright notice, this list of conditions and the
    |      following disclaimer.
    | 
    |   o  Redistributions in binary form must reproduce the
    |      above copyright notice, this list of conditions and
    |      the following disclaimer in the documentation and/or
    |      other materials provided with the distribution.
    |
    |   o  Neither the name of Deutsches Elektronen-Synchrotron
    |      (DESY) nor the names of its contributors may be used
    |      to endorse or promote products derived from this
    |      software without specific prior written permission.
    |
    | THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
    | CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
    | INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
    | MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    | DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
    | CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    | SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
    | NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    | LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
    | HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
    | CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
    | OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
    | SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
    +-->


<!--+
    |  Xylophone - convert XML data into LDIF
    |
    |  Parameters
    |    xml-src-uri:  the URI for the dynamic XML data
    +-->

<xsl:stylesheet version="1.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
		xmlns:date="http://exslt.org/dates-and-times"
		extension-element-prefixes="date">


<!-- TODO: what is the MIME media type for LDIF?  This seems poorly
     defined -->
<xsl:output method="text" media-type="text/plain"/>

<xsl:param name="xml-src-uri" select="'dynamic.xml'"/>

<xsl:strip-space elements="*"/>

<xsl:include href="xylophone-publish.xsl"/>
<xsl:include href="xylophone-map.xsl"/>
<xsl:include href="xylophone-output.xsl"/>
<xsl:include href="xylophone-predicate.xsl"/>
<xsl:include href="xylophone-import.xsl"/>
<xsl:include href="xylophone-user-elements.xsl"/>
<xsl:include href="xylophone-path.xsl"/>
<xsl:include href="xylophone-markup.xsl"/>


<!--+
    |     Main entry point.
    +-->
<xsl:template match="/">
  <xsl:call-template name="output-preamble"/>

  <xsl:apply-templates select="/xylophone/publish" mode="publish"/>
</xsl:template>



<!--+
    |  Output the preamble at the start of the LDIF file.
    +-->
<xsl:template name="output-preamble">

  <!-- Empty comment line -->
  <xsl:call-template name="output-comment"/>

  <xsl:call-template name="output-comment">
    <xsl:with-param name="text" select="'LDIF generated by Xylophone v0.1'"/>
  </xsl:call-template>

  <!-- Empty comment line -->
  <xsl:call-template name="output-comment"/>

  <!-- Add some system info -->
  <xsl:call-template name="output-comment">
    <xsl:with-param name="text">XSLT processing using <xsl:value-of select="concat(system-property('xsl:vendor'), ' ', system-property('xsl:version'))"/> (<xsl:value-of select="system-property('xsl:vendor-url')"/>)</xsl:with-param>
  </xsl:call-template>

  <!-- Add the timestamp -->
  <xsl:choose>
    <xsl:when test="function-available('date:date-time')">
      <xsl:call-template name="output-comment">
	<xsl:with-param name="text">
	  <xsl:value-of select="concat(' at: ', date:date-time())" />
	</xsl:with-param>
      </xsl:call-template>
    </xsl:when>

    <xsl:otherwise>
      <!-- Implementation has no date-time() function. -->
    </xsl:otherwise>
  </xsl:choose>


  <!-- Empty comment line -->
  <xsl:call-template name="output-comment"/>

  <!-- Emit a blank line -->
  <xsl:call-template name="output-EOL"/>
</xsl:template>


</xsl:stylesheet>


